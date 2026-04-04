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
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import ApiClient
from .const import (
    BACKFILL_DAYS,
    DOMAIN,
    RETRY_DAYS,
    SCAN_INTERVAL,
    STAT_ELECTRICITY_CONSUMED,
    STAT_ELECTRICITY_COST,
    STAT_ELECTRICITY_RETURNED,
    STAT_GAS_CONSUMED,
    STAT_GAS_COST,
)
from .model import GetMeterReadingsRequest, MeterReadingEntry

_LOGGER = logging.getLogger(__name__)

# Dutch local timezone — all hour labels from the API are in this zone.
_TZ_NL = ZoneInfo("Europe/Amsterdam")

# Ordered tuple of all statistic IDs managed by this integration.
_ALL_STAT_IDS = (
    STAT_ELECTRICITY_CONSUMED,
    STAT_ELECTRICITY_RETURNED,
    STAT_GAS_CONSUMED,
    STAT_ELECTRICITY_COST,
    STAT_GAS_COST,
)

# Maps statistic_id → (friendly name, unit of measurement, unit class)
_STAT_META: dict[str, tuple[str, str, str | None]] = {
    STAT_ELECTRICITY_CONSUMED: (
        "Coolblue Electricity Consumed",
        UnitOfEnergy.KILO_WATT_HOUR,
        "energy",
    ),
    STAT_ELECTRICITY_RETURNED: (
        "Coolblue Electricity Returned",
        UnitOfEnergy.KILO_WATT_HOUR,
        "energy",
    ),
    STAT_GAS_CONSUMED: ("Coolblue Gas Consumed", UnitOfVolume.CUBIC_METERS, "volume"),
    STAT_ELECTRICITY_COST: ("Coolblue Electricity Cost", CURRENCY_EURO, None),
    STAT_GAS_COST: ("Coolblue Gas Cost", CURRENCY_EURO, None),
}


# ── Public data model ─────────────────────────────────────────────────────────


@dataclass
class CoordinatorData:
    """Yesterday's hourly readings, used by sensor entities for display."""

    electricity: list[MeterReadingEntry]
    gas: list[MeterReadingEntry]
    costs: list[MeterReadingEntry] = field(default_factory=list)
    """Hourly cost breakdown (from the separate 'costs' API request)."""


# ── Helpers ───────────────────────────────────────────────────────────────────


def _day_start_utc(day: date) -> datetime:
    """Return the UTC datetime for midnight of *day* in Amsterdam local time."""
    return datetime(day.year, day.month, day.day, 0, 0, tzinfo=_TZ_NL).astimezone(
        timezone.utc
    )


def _entry_to_utc(entry_name: str, for_date: date) -> datetime:
    """Convert an API hour label (``"HH:MM"``) plus a date to a UTC datetime."""
    hour, minute = map(int, entry_name.split(":"))
    local_dt = datetime(
        for_date.year, for_date.month, for_date.day, hour, minute, tzinfo=_TZ_NL
    )
    return local_dt.astimezone(timezone.utc)


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
                # Back-fill history; yesterday is included as the last day.
                data = await self._async_backfill(BACKFILL_DAYS)
                self._backfilled = True
                return data

            # Normal refresh: re-check the last RETRY_DAYS days.
            # Coolblue sometimes publishes data late, so we look back further
            # than just yesterday to catch any days that were empty before.
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
                    seed_sums = None  # force DB re-query for the next day
                    continue

                seed_sums = await self._inject_statistics(
                    electricity, gas, day, seed_sums
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
        """
        Re-fetch and re-inject the last *days* days (oldest first).

        Used by the regular polling path.  If the API is completely down
        (every day fails) the last exception is re-raised so HA can mark the
        integration as unavailable.  Partial failures are swallowed.
        """
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range, raise_if_all_fail=True)

    async def _async_backfill(self, days: int) -> CoordinatorData:
        """
        Inject the last *days* days of history (oldest first).

        Returns a ``CoordinatorData`` for immediate sensor display.
        """
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range)

    async def async_reimport_statistics(self, start_date: date) -> None:
        """
        Reimport all statistics from *start_date* through yesterday (inclusive).

        Fetches hourly data day-by-day, overwrites any existing statistics in
        the recorder for the range and recalculates cumulative sums.  Use this
        to fix gaps, negative spikes, or other artefacts in the Energy Dashboard.
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
        await self._async_process_day_range(day_range)

        _LOGGER.info("Statistics reimport complete.")
        await self.async_refresh()

    async def _fetch_day(
        self, day: date
    ) -> tuple[list[MeterReadingEntry], list[MeterReadingEntry], list[MeterReadingEntry]]:
        """Fetch hourly electricity, gas and cost data for *day* from the API.

        Each energy type is fetched independently so that a missing contract
        (electricity-only or gas-only account) does not prevent the other type
        from being ingested.  The cost request is always treated as optional —
        its failure never causes the day to be skipped.  Only when *both*
        electricity and gas fail is an exception raised.
        """
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

        # Both consumption types failed — propagate so the caller can handle it.
        if electricity_exception is not None and gas_exception is not None:
            raise electricity_exception

        return electricity, gas, costs

    async def _get_sum_before(self, stat_id: str, before_dt: datetime) -> float:
        """
        Return the last recorded cumulative sum strictly before *before_dt*.

        Queries a 25-hour window to cover DST transition days (23/25 h days).
        Falls back to ``0.0`` if no prior statistics exist.
        """
        query_start = before_dt - timedelta(hours=25)
        result = await get_instance(self.hass).async_add_executor_job(
            lambda: statistics_during_period(
                self.hass,
                query_start,
                before_dt,
                {stat_id},
                "hour",
                None,
                {"sum"},
            ),
        )
        entries = result.get(stat_id, [])
        if entries:
            return entries[-1].get("sum") or 0.0
        return 0.0

    async def _inject_statistics(
        self,
        electricity_entries: list[MeterReadingEntry],
        gas_entries: list[MeterReadingEntry],
        for_date: date,
        seed_sums: dict[str, float] | None,
    ) -> dict[str, float]:
        """
        Build ``StatisticData`` objects for one day and hand them to the recorder.

        *seed_sums* provides the running total at the start of the day.
        If ``None``, the value is queried from the recorder (for daily refreshes
        and after a backfill failure).

        Returns the updated sums at the end of the day for use as seeds for
        the next day (useful during backfill to avoid redundant DB queries).
        """
        day_start_utc = _day_start_utc(for_date)

        # (stat_id, entries, value extractor)
        stat_configs = [
            (
                STAT_ELECTRICITY_CONSUMED,
                electricity_entries,
                lambda e: e.electricity.total,
            ),
            (
                STAT_ELECTRICITY_RETURNED,
                electricity_entries,
                lambda e: e.production.total,
            ),
            (STAT_GAS_CONSUMED, gas_entries, lambda e: e.gas),
            (
                STAT_ELECTRICITY_COST,
                electricity_entries,
                lambda e: e.costs.electricity.total,
            ),
            (STAT_GAS_COST, gas_entries, lambda e: e.costs.gas.total),
        ]

        result_sums: dict[str, float] = {}

        for stat_id, entries, value_fn in stat_configs:
            # Determine the running sum at the start of this day.
            if seed_sums is not None and stat_id in seed_sums:
                running_sum = seed_sums[stat_id]
            else:
                running_sum = await self._get_sum_before(stat_id, day_start_utc)

            stat_data: list[StatisticData] = []
            for entry in entries:
                start_utc = _entry_to_utc(entry.name, for_date)
                delta = value_fn(entry)
                running_sum += delta
                stat_data.append(
                    StatisticData(start=start_utc, state=delta, sum=running_sum)
                )

            if stat_data:
                name, unit, unit_class = _STAT_META[stat_id]
                metadata = StatisticMetaData(
                    mean_type=StatisticMeanType.NONE,
                    has_sum=True,
                    name=name,
                    source=DOMAIN,
                    statistic_id=stat_id,
                    unit_class=unit_class,
                    unit_of_measurement=unit,
                )
                async_add_external_statistics(self.hass, metadata, stat_data)

            result_sums[stat_id] = running_sum

        return result_sums
