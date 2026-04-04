"""Shared fixtures and factory helpers for Coolblue Energy tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.coolblue_energy.model import (
    CostComponent,
    HourlyCosts,
    MeterReadingEntry,
    PeakUsage,
    SmartDeviceUsage,
)

# ── Factory helpers ───────────────────────────────────────────────────────────


def make_electricity_entry(
    hour: int,
    electricity: float = 1.0,
    production: float = 0.2,
    price: float = 0.25,
    electricity_cost: float = 0.25,
) -> MeterReadingEntry:
    """Create one hourly electricity-type MeterReadingEntry."""
    return MeterReadingEntry(
        name=f"{hour:02d}:00",
        electricity=PeakUsage(total=electricity, off_peak=0.6, peak=0.4),
        production=PeakUsage(total=production, off_peak=0.0, peak=0.2),
        costs=HourlyCosts(
            electricity=CostComponent(
                total=electricity_cost,
                fixed=0.01,
                consumption=round(electricity_cost - 0.01, 6),
            ),
            gas=CostComponent(total=0.10, fixed=0.01, consumption=0.09),
            production=-0.05,
        ),
        smart_device_usage=SmartDeviceUsage(
            free=0.0, paid=0.0, has_free_drying=False, has_free_washing=False
        ),
        price=price,
        gas=0.0,
    )


def make_gas_entry(hour: int, gas: float = 0.05) -> MeterReadingEntry:
    """Create one hourly gas-type MeterReadingEntry."""
    return MeterReadingEntry(
        name=f"{hour:02d}:00",
        electricity=PeakUsage(total=0.0, off_peak=0.0, peak=0.0),
        production=PeakUsage(total=0.0, off_peak=0.0, peak=0.0),
        costs=HourlyCosts(
            electricity=CostComponent(total=0.0, fixed=0.0, consumption=0.0),
            gas=CostComponent(total=0.10, fixed=0.01, consumption=0.09),
            production=0.0,
        ),
        smart_device_usage=SmartDeviceUsage(
            free=0.0, paid=0.0, has_free_drying=False, has_free_washing=False
        ),
        price=0.0,
        gas=gas,
    )


def make_cost_entry(
    hour: int,
    electricity_cost: float = 0.25,
    gas_cost: float = 0.10,
) -> MeterReadingEntry:
    """Create one hourly costs-type MeterReadingEntry (from the 'costs' API request).

    The electricity and gas consumption fields are 0 — only the cost breakdown
    fields carry meaningful data in this response type.
    """
    return MeterReadingEntry(
        name=f"{hour:02d}:00",
        electricity=PeakUsage(total=0.0, off_peak=0.0, peak=0.0),
        production=PeakUsage(total=0.0, off_peak=0.0, peak=0.0),
        costs=HourlyCosts(
            electricity=CostComponent(
                total=electricity_cost,
                fixed=-0.01,
                consumption=round(electricity_cost + 0.01, 6),
            ),
            gas=CostComponent(
                total=gas_cost,
                fixed=0.04,
                consumption=round(gas_cost - 0.04, 6),
            ),
            production=0.0,
        ),
        smart_device_usage=SmartDeviceUsage(
            free=0.0, paid=0.0, has_free_drying=False, has_free_washing=False
        ),
        price=0.0,
        gas=0.0,
    )


def make_day_electricity(
    n_hours: int = 24, electricity: float = 1.0, production: float = 0.2
) -> list[MeterReadingEntry]:
    """Return *n_hours* uniform electricity entries."""
    return [
        make_electricity_entry(h, electricity=electricity, production=production)
        for h in range(n_hours)
    ]


def make_day_gas(n_hours: int = 24, gas: float = 0.05) -> list[MeterReadingEntry]:
    """Return *n_hours* uniform gas entries."""
    return [make_gas_entry(h, gas=gas) for h in range(n_hours)]


def make_day_costs(
    n_hours: int = 24, electricity_cost: float = 0.25, gas_cost: float = 0.10
) -> list[MeterReadingEntry]:
    """Return *n_hours* uniform cost entries (from the 'costs' API request)."""
    return [
        make_cost_entry(h, electricity_cost=electricity_cost, gas_cost=gas_cost)
        for h in range(n_hours)
    ]


# ── Pytest fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def fake_electricity() -> list[MeterReadingEntry]:
    """24 electricity entries: 1.0 kWh/h consumed, 0.2 kWh/h produced."""
    return make_day_electricity()


@pytest.fixture
def fake_gas() -> list[MeterReadingEntry]:
    """24 gas entries: 0.05 m³/h consumed."""
    return make_day_gas()


@pytest.fixture
def fake_costs() -> list[MeterReadingEntry]:
    """24 cost entries: €0.25/h electricity cost, €0.10/h gas cost."""
    return make_day_costs()


@pytest.fixture
def mock_api_client(fake_electricity, fake_gas, fake_costs) -> AsyncMock:
    """AsyncMock ApiClient that returns fake entries based on energy_type."""
    client = AsyncMock()
    client.get_energy_ids.return_value = (
        "00844083",
        "3addb383-a979-40b4-8487-0f3bc0854da5",
    )

    def _side_effect(req):
        if req.energy_type == "electricity":
            return fake_electricity
        if req.energy_type == "gas":
            return fake_gas
        return fake_costs  # "costs"

    client.get_hourly_energy.side_effect = _side_effect
    return client


@pytest.fixture
def mock_hass() -> MagicMock:
    """Minimal mock HomeAssistant: async_add_executor_job calls the lambda."""
    hass = MagicMock()

    async def executor_job(fn, *args):
        return fn(*args) if args else fn()

    hass.async_add_executor_job = executor_job

    # get_instance(hass) must return a recorder-like object with the same
    # executor so coordinator._get_sum_before works in tests.
    mock_recorder = MagicMock()
    mock_recorder.async_add_executor_job = executor_job
    hass._mock_recorder = mock_recorder  # keep a reference for patching

    return hass


@pytest.fixture(autouse=True)
def patch_get_instance(mock_hass):
    """Patch coordinator.get_instance to return mock_hass._mock_recorder."""
    from unittest.mock import patch

    with patch(
        "custom_components.coolblue_energy.coordinator.get_instance",
        return_value=mock_hass._mock_recorder,
    ):
        yield


@pytest.fixture
def coordinator(mock_hass, mock_api_client):
    """
    CoolblueCoordinator with the HA DataUpdateCoordinator infrastructure
    bypassed via ``object.__new__``.
    """
    from custom_components.coolblue_energy.coordinator import CoolblueCoordinator

    coord = object.__new__(CoolblueCoordinator)
    coord.hass = mock_hass
    coord._client = mock_api_client
    coord._debtor_id = "00844083"
    coord._location_id = "3addb383-a979-40b4-8487-0f3bc0854da5"
    coord._backfilled = False
    return coord
