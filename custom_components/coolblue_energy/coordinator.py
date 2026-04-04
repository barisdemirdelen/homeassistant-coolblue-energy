"""
coordinator.py

DataUpdateCoordinator for Coolblue Energy.

Fetches yesterday's hourly electricity and gas data and injects it as
long-term external statistics into the HA recorder so the Energy Dashboard
can display it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
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
from homeassistant.const import UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api_client import ApiClient
from .const import (
    BACKFILL_DAYS,
    DOMAIN,
    RETRY_DAYS,
    SCAN_INTERVAL,
    STAT_ELECTRICITY_CONSUMED,
    STAT_ELECTRICITY_RETURNED,
    STAT_GAS_CONSUMED,
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
)

# Maps statistic_id → (friendly name, unit of measurement, unit class)
_STAT_META: dict[str, tuple[str, str, str]] = {
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
}


# ── Public data model ─────────────────────────────────────────────────────────


@dataclass
class CoordinatorData:
    """Yesterday's hourly readings, used by sensor entities for display."""

    electricity: list[MeterReadingEntry]
    gas: list[MeterReadingEntry]


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

    async def _async_retry_recent_days(self, days: int) -> CoordinatorData:
        """
        Re-fetch and re-inject the last *days* days (oldest first).

        Handles delayed Coolblue data: if a day returns no entries it is logged
        and skipped so it will be retried on the next poll.  Days that already
        have statistics are safe to re-inject (idempotent).

        If every single fetch attempt raises an exception (e.g. the API is
        completely down) the last exception is re-raised so the caller can
        convert it to ``UpdateFailed``.  Partial failures (some days succeed,
        some fail) are logged and swallowed — we still return the data we got.

        Returns a ``CoordinatorData`` with the most recent day that had data
        (usually yesterday).
        """
        seed_sums: dict[str, float] | None = None
        last_data: CoordinatorData | None = None
        any_success = False
        last_exc: Exception | None = None

        for offset in range(days, 0, -1):
            day = date.today() - timedelta(days=offset)
            try:
                electricity, gas = await self._fetch_day(day)
                any_success = True
                if not electricity and not gas:
                    _LOGGER.debug(
                        "No data available yet for %s — will retry on next poll.", day
                    )
                    # Reset seed so the next day queries the DB for a safe baseline.
                    seed_sums = None
                    continue

                seed_sums = await self._inject_statistics(
                    electricity, gas, day, seed_sums
                )
                last_data = CoordinatorData(electricity=electricity, gas=gas)
            except Exception as exc:
                _LOGGER.warning("Refresh failed for %s, skipping.", day, exc_info=True)
                seed_sums = None
                last_exc = exc

        # If every attempt raised an exception re-raise so the outer handler
        # can wrap it in UpdateFailed and HA shows the integration as unavailable.
        if not any_success and last_exc is not None:
            raise last_exc

        # If no day had data at all, return an empty payload — sensors will
        # keep their last known state.
        return last_data or CoordinatorData(electricity=[], gas=[])

    async def _fetch_day(
        self, day: date
    ) -> tuple[list[MeterReadingEntry], list[MeterReadingEntry]]:
        """Fetch hourly electricity and gas data for *day* from the API."""
        electricity = await self._client.get_hourly_energy(
            GetMeterReadingsRequest(
                customer_id=self._debtor_id,
                connection_uuid=self._location_id,
                energy_type="electricity",
                for_date=day,
            )
        )
        gas = await self._client.get_hourly_energy(
            GetMeterReadingsRequest(
                customer_id=self._debtor_id,
                connection_uuid=self._location_id,
                energy_type="gas",
                for_date=day,
            )
        )
        return electricity, gas

    async def _async_backfill(self, days: int) -> CoordinatorData:
        """
        Inject the last *days* days of history (oldest first).

        Returns yesterday's ``CoordinatorData`` for immediate sensor display.
        """
        first_day = date.today() - timedelta(days=days)
        first_dt_utc = _day_start_utc(first_day)

        # Seed running sums from the last recorded entry *before* our window.
        seed_sums: dict[str, float] | None = {
            stat_id: await self._get_sum_before(stat_id, first_dt_utc)
            for stat_id in _ALL_STAT_IDS
        }

        last_electricity: list[MeterReadingEntry] = []
        last_gas: list[MeterReadingEntry] = []

        for offset in range(days, 0, -1):
            day = date.today() - timedelta(days=offset)
            try:
                electricity, gas = await self._fetch_day(day)
                seed_sums = await self._inject_statistics(
                    electricity, gas, day, seed_sums
                )
                if offset == 1:  # yesterday — keep for sensor state
                    last_electricity, last_gas = electricity, gas
            except Exception:
                _LOGGER.warning("Backfill failed for %s, skipping.", day, exc_info=True)
                # Force a fresh DB query for the next day so sums stay correct.
                seed_sums = None

        return CoordinatorData(electricity=last_electricity, gas=last_gas)

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
