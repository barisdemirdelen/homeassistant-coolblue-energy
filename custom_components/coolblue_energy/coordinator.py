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
from datetime import date, timedelta

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import ApiClient
from .const import (
    BACKFILL_DAYS,
    DOMAIN,
    RETRY_DAYS,
    SCAN_INTERVAL,
)
from .model import GetMeterReadingsRequest, MeterReadingEntry
from .recorder import async_inject_day
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


class CoolblueCoordinator(DataUpdateCoordinator[CoordinatorData]):
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
        super().__init__(hass, _LOGGER, name=DOMAIN, update_interval=SCAN_INTERVAL)
        self._client = client
        self._debtor_id = debtor_id
        self._location_id = location_id
        self._backfilled = False

    # ── DataUpdateCoordinator hook ────────────────────────────────────────────

    async def _async_update_data(self) -> CoordinatorData:
        try:
            if not self._backfilled:
                data = await self._async_backfill(BACKFILL_DAYS)
                self._backfilled = True
                return data

            return await self._async_retry_recent_days(RETRY_DAYS)

        except Exception as err:
            raise UpdateFailed(f"Error fetching Coolblue data: {err}") from err

    # ── Private helpers ───────────────────────────────────────────────────────

    async def _async_process_day_range(
        self,
        days: list[date],
        *,
        raise_if_all_fail: bool = False,
    ) -> CoordinatorData:
        """
        Core loop: fetch and inject statistics for *days* (oldest first).

        Seed sums are seeded lazily from the DB on the first day that has data
        and chained to subsequent days without extra DB queries.

        Empty days (API returns no entries yet) are skipped and the seed is
        reset so the next day re-queries the DB for a safe baseline.  Partial
        fetch failures are logged and the loop continues.

        If *raise_if_all_fail* is ``True`` and every attempt raises an
        exception, the last exception is re-raised so the caller can convert
        it to ``UpdateFailed`` (used by the regular polling path).

        Returns a ``CoordinatorData`` for the most recent day that had data,
        or an empty ``CoordinatorData`` if no day returned any entries.
        """
        seed_sums: dict[str, float] | None = None
        last_data: CoordinatorData | None = None
        any_success = False
        last_exc: Exception | None = None

        for day in days:
            try:
                electricity, gas, costs = await self._fetch_day(day)
                any_success = True
                if not electricity and not gas:
                    _LOGGER.debug(
                        "No data available yet for %s — will retry on next poll.", day
                    )
                    seed_sums = None
                    continue

                seed_sums = await self._inject_statistics(
                    electricity, gas, costs, day, seed_sums
                )
                last_data = CoordinatorData(
                    electricity=electricity, gas=gas, costs=costs
                )
            except Exception as exc:
                _LOGGER.warning(
                    "Failed to fetch data for %s, skipping.", day, exc_info=True
                )
                seed_sums = None
                last_exc = exc

        if raise_if_all_fail and not any_success and last_exc is not None:
            raise last_exc

        return last_data or CoordinatorData(electricity=[], gas=[])

    async def _async_retry_recent_days(self, days: int) -> CoordinatorData:
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range, raise_if_all_fail=True)

    async def _async_backfill(self, days: int) -> CoordinatorData:
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range)

    async def async_reimport_statistics(self, start_date: date) -> None:
        """
        Reimport all statistics from *start_date* through yesterday (inclusive).
        """
        yesterday = date.today() - timedelta(days=1)
        if start_date > yesterday:
            _LOGGER.warning(
                "Reimport start_date %s is not before today — nothing to do.",
                start_date,
            )
            return

        total_days = (yesterday - start_date).days + 1
        _LOGGER.info(
            "Starting statistics reimport from %s to %s (%d days).",
            start_date,
            yesterday,
            total_days,
        )

        day_range = [start_date + timedelta(days=i) for i in range(total_days)]
        result = await self._async_process_day_range(day_range)

        _LOGGER.info("Statistics reimport complete.")
        self.async_set_updated_data(result)

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
