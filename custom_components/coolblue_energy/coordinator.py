"""
coordinator.py

DataUpdateCoordinator for Coolblue Energy.

Fetches yesterday's hourly electricity and gas data and injects it as
long-term external statistics into the HA recorder so the Energy Dashboard
can display it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .api_client import ApiClient
from .const import (
    BACKFILL_DAYS,
    DOMAIN,
    RETRY_DAYS,
    SCAN_INTERVAL,
)
from .ha_external_statistics.recorder import async_inject_day
from .ha_external_statistics.statistics_mixin import StatisticsLoopMixin
from .model import GetMeterReadingsRequest, MeterReadingEntry
from .statistics import (
    ELECTRICITY_CONSUMED,
    ELECTRICITY_COST,
    ELECTRICITY_RETURNED,
    ELECTRICITY_RETURNED_COMPENSATION,
    GAS_CONSUMED,
    GAS_COST,
    _day_start_utc,
)

_LOGGER = logging.getLogger(__name__)


# ── Public data model ─────────────────────────────────────────────────────────


@dataclass
class CoordinatorData:
    """Yesterday's hourly readings, kept for callers that still need raw entries."""

    electricity: list[MeterReadingEntry]
    gas: list[MeterReadingEntry]
    costs: list[MeterReadingEntry] = field(default_factory=list)


# ── Coordinator ───────────────────────────────────────────────────────────────


class CoolblueCoordinator(StatisticsLoopMixin, DataUpdateCoordinator[CoordinatorData]):
    """
    Fetches Coolblue Energy data every ``SCAN_INTERVAL`` hours.

    On the first run it back-fills ``BACKFILL_DAYS`` days of history; on every
    subsequent run it (re-)injects yesterday's statistics so the Energy
    Dashboard is always up to date.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        client: ApiClient,
        debtor_id: str,
        location_id: str,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
            backfill_days=BACKFILL_DAYS,
            retry_days=RETRY_DAYS,
        )
        self._client = client
        self._debtor_id = debtor_id
        self._location_id = location_id
        self._last_data: CoordinatorData = CoordinatorData(electricity=[], gas=[])

    # ── DataUpdateCoordinator hook ────────────────────────────────────────────

    async def _async_update_data(self) -> CoordinatorData:
        await self.async_run_statistics_update()
        return self._last_data

    async def async_reimport_statistics(self, start_date: date) -> None:
        """Reimport statistics and refresh coordinator data."""
        await super().async_reimport_statistics(start_date)
        self.async_set_updated_data(self._last_data)

    # ── StatisticsLoopMixin hook ──────────────────────────────────────────────

    async def _process_day(
        self,
        day: date,
        seed_sums: dict[str, float] | None,
    ) -> dict[str, float] | None:
        """Fetch and inject one day; return None if no data yet."""
        electricity, gas, costs = await self._fetch_day(day)
        if not electricity and not gas:
            _LOGGER.debug(
                "No data available yet for %s — will retry on next poll.", day
            )
            return None

        self._last_data = CoordinatorData(electricity=electricity, gas=gas, costs=costs)
        return await self._inject_statistics(electricity, gas, costs, day, seed_sums)

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _fetch_day(
        self, day: date
    ) -> tuple[
        list[MeterReadingEntry], list[MeterReadingEntry], list[MeterReadingEntry]
    ]:
        """Fetch hourly electricity, gas and cost data for *day* from the API."""
        electricity: list[MeterReadingEntry] = []
        gas: list[MeterReadingEntry] = []
        costs: list[MeterReadingEntry] = []
        electricity_exception: Exception | None = None
        gas_exception: Exception | None = None

        try:
            electricity = await self._client.get_hourly_energy(
                GetMeterReadingsRequest(
                    customer_id=self._debtor_id,
                    connection_uuid=self._location_id,
                    energy_type="electricity",
                    for_date=day,
                )
            )
        except Exception as exc:
            electricity_exception = exc
            _LOGGER.debug("Could not fetch electricity data for %s: %s", day, exc)

        try:
            gas = await self._client.get_hourly_energy(
                GetMeterReadingsRequest(
                    customer_id=self._debtor_id,
                    connection_uuid=self._location_id,
                    energy_type="gas",
                    for_date=day,
                )
            )
        except Exception as exc:
            gas_exception = exc
            _LOGGER.debug("Could not fetch gas data for %s: %s", day, exc)

        try:
            costs = await self._client.get_hourly_energy(
                GetMeterReadingsRequest(
                    customer_id=self._debtor_id,
                    connection_uuid=self._location_id,
                    energy_type="costs",
                    for_date=day,
                )
            )
        except Exception as exc:
            _LOGGER.debug("Could not fetch cost data for %s: %s", day, exc)

        if electricity_exception is not None and gas_exception is not None:
            raise electricity_exception

        return electricity, gas, costs

    async def _inject_statistics(
        self,
        electricity_entries: list[MeterReadingEntry],
        gas_entries: list[MeterReadingEntry],
        costs_entries: list[MeterReadingEntry],
        for_date: date,
        seed_sums: dict[str, float] | None,
    ) -> dict[str, float]:
        """
        Inject statistics for one day via each ``ExternalStatistic`` instance.

        Cost statistics are only injected when the matching consumption contract
        is present (i.e. ``electricity_entries`` / ``gas_entries`` is non-empty).

        *seed_sums* provides the running total at the start of the day.
        If ``None``, each statistic queries the recorder for its seed value.

        Returns the updated sums at the end of the day for chaining.
        """
        costs = costs_entries if electricity_entries else []
        return await async_inject_day(
            self.hass,
            [
                (ELECTRICITY_CONSUMED, electricity_entries),
                (ELECTRICITY_RETURNED, electricity_entries),
                (GAS_CONSUMED, gas_entries),
                (ELECTRICITY_COST, costs),
                (ELECTRICITY_RETURNED_COMPENSATION, costs),
                (GAS_COST, costs_entries if gas_entries else []),
            ],
            for_date,
            _day_start_utc(for_date),
            seed_sums,
        )
