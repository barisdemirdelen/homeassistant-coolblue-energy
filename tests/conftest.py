"""Shared fixtures and factory helpers for Coolblue Energy tests."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock

import pytest
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.coolblue_energy.const import (
    CONF_DEBTOR_ID,
    CONF_LOCATION_ID,
    DEFAULT_NAME,
    DOMAIN,
)
from custom_components.coolblue_energy.coordinator import CoolblueCoordinator
from custom_components.coolblue_energy.model import (
    AmountData,
    ElectricityData,
    FeedInData,
    GasData,
    MeterReadingEntry,
    PeakUsage,
)

# The Home Assistant test plugin calls ``logging.basicConfig(level=INFO)`` and
# puts ``sqlalchemy.engine`` at INFO at import time, which echoes every recorder
# statement to a stream pytest cannot capture. Undo it — conftest is imported
# after plugins, so this wins.
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

# ── Factory helpers ───────────────────────────────────────────────────────────

# The account every test acts on: one debtor, one metered location.
DEBTOR_ID = "00844083"
LOCATION_ID = "3addb383-a979-40b4-8487-0f3bc0854da5"

ENTRY_DATA = {
    CONF_EMAIL: "user@example.com",
    CONF_PASSWORD: "hunter2",
    CONF_DEBTOR_ID: DEBTOR_ID,
    CONF_LOCATION_ID: LOCATION_ID,
}

_FAKE_DATE = "2026-01-01"


def _ts(hour: int) -> str:
    return f"{_FAKE_DATE}T{hour:02d}:00:00.000Z"


def make_electricity_entry(
    hour: int,
    electricity: float = 1.0,
    production: float = 0.2,
    price: float = 0.25,
    electricity_cost: float = 0.25,
) -> MeterReadingEntry:
    """Create one hourly electricity-type MeterReadingEntry."""
    return MeterReadingEntry(
        timestamp=_ts(hour),
        electricity=ElectricityData(
            usage=PeakUsage(total=electricity, off_peak=0.6, peak=0.4),
            cost=AmountData(amount=electricity_cost),
        ),
        gas=GasData(usage=0.0),
        feed_in=FeedInData(
            production=PeakUsage(total=production, off_peak=production, peak=0.0),
            cost=AmountData(amount=-0.05),
        ),
        dynamic_price=price,
    )


def make_gas_entry(hour: int, gas: float = 0.05) -> MeterReadingEntry:
    """Create one hourly gas-type MeterReadingEntry."""
    return MeterReadingEntry(
        timestamp=_ts(hour),
        gas=GasData(usage=gas),
    )


def make_cost_entry(
    hour: int,
    electricity_cost: float = 0.25,
    gas_cost: float = 0.10,
    production_cost: float = 0.0,
) -> MeterReadingEntry:
    """Create one hourly costs-type MeterReadingEntry (from the 'costs' API request).

    The electricity and gas consumption fields are 0 — only the cost breakdown
    fields carry meaningful data in this response type.
    """
    return MeterReadingEntry(
        timestamp=_ts(hour),
        electricity=ElectricityData(cost=AmountData(amount=electricity_cost)),
        gas=GasData(cost=AmountData(amount=gas_cost)),
        feed_in=FeedInData(cost=AmountData(amount=production_cost))
        if production_cost
        else None,
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
    n_hours: int = 24,
    electricity_cost: float = 0.25,
    gas_cost: float = 0.10,
    production_cost: float = 0.0,
) -> list[MeterReadingEntry]:
    """Return *n_hours* uniform cost entries (from the 'costs' API request)."""
    return [
        make_cost_entry(
            h,
            electricity_cost=electricity_cost,
            gas_cost=gas_cost,
            production_cost=production_cost,
        )
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
    client.get_energy_ids.return_value = (DEBTOR_ID, LOCATION_ID)

    def _side_effect(req):
        if req.energy_type == "electricity":
            return fake_electricity
        if req.energy_type == "gas":
            return fake_gas
        return fake_costs  # "costs"

    client.get_hourly_energy.side_effect = _side_effect
    return client


@pytest.fixture
def config_entry() -> MockConfigEntry:
    """A Coolblue Energy config entry with credentials already resolved."""
    return MockConfigEntry(
        domain=DOMAIN,
        title=f"{DEFAULT_NAME} (debtor {DEBTOR_ID})",
        data=ENTRY_DATA,
        unique_id=DEBTOR_ID,
    )


@pytest.fixture
def coordinator(
    recorder_mock: None,
    hass: HomeAssistant,
    config_entry: MockConfigEntry,
    mock_api_client: AsyncMock,
) -> CoolblueCoordinator:
    """A real ``CoolblueCoordinator`` on a real Home Assistant, backed by a real recorder.

    ``recorder_mock`` must be requested before anything that reads statistics:
    it is what makes ``get_instance(hass)`` resolve to a running recorder.
    """
    config_entry.add_to_hass(hass)
    return CoolblueCoordinator(hass, config_entry, mock_api_client)
