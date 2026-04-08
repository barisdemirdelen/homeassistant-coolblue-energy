"""
base_coordinator.py

Generic DataUpdateCoordinator base class that manages backfill, retry-recent,
and reimport loops.

Provider-specific subclasses implement ``_process_day`` to fetch and inject
data for a single calendar day, and ``_make_empty_data`` to provide a
zero-data fallback when no day had any readings.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import date, timedelta
from typing import Generic, TypeVar

from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

DataT = TypeVar("DataT")


class EnergyCoordinatorBase(DataUpdateCoordinator[DataT], Generic[DataT]):
    """
    Coordinator base that owns the backfill / retry / reimport control flow.

    Subclasses must implement:

    * ``_process_day`` — fetch + inject one calendar day.
    * ``_make_empty_data`` — zero-data fallback when no day had readings.
    """

    @abstractmethod
    async def _process_day(
        self,
        day: date,
        seed_sums: dict[str, float] | None,
    ) -> tuple[dict[str, float] | None, DataT | None]:
        """
        Fetch and inject statistics for *day*.

        Returns ``(new_seed_sums, data)`` on success, or ``(None, None)``
        when the API has no data yet for this day (pending / empty).
        Raise on unrecoverable errors.
        """

    @abstractmethod
    def _make_empty_data(self) -> DataT:
        """Return a zero/empty data object used when no day had any readings."""

    # ── Core loop ─────────────────────────────────────────────────────────────

    async def _async_process_day_range(
        self,
        days: list[date],
        *,
        raise_if_all_fail: bool = False,
    ) -> DataT:
        """
        Iterate *days* (oldest first), calling ``_process_day`` for each.

        Seed sums are chained across consecutive successful days.  Empty days
        and exceptions both reset the seed so the next day re-queries the DB.

        When *raise_if_all_fail* is ``True`` and every attempt raised an
        exception, the last exception is re-raised.
        """
        seed_sums: dict[str, float] | None = None
        last_data: DataT | None = None
        any_success = False
        last_exc: Exception | None = None

        for day in days:
            try:
                seed_sums, data = await self._process_day(day, seed_sums)
                any_success = True
                if data is None:
                    seed_sums = None
                    continue
                last_data = data
            except Exception as exc:
                _LOGGER.warning(
                    "Failed to fetch data for %s, skipping.", day, exc_info=True
                )
                seed_sums = None
                last_exc = exc

        if raise_if_all_fail and not any_success and last_exc is not None:
            raise last_exc

        return last_data if last_data is not None else self._make_empty_data()

    # ── Scheduling helpers ────────────────────────────────────────────────────

    async def _async_retry_recent_days(self, days: int) -> DataT:
        """Re-inject the last *days* calendar days, raising if all fail."""
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range, raise_if_all_fail=True)

    async def _async_backfill(self, days: int) -> DataT:
        """Inject the last *days* calendar days, silently skipping failures."""
        day_range = [
            date.today() - timedelta(days=offset) for offset in range(days, 0, -1)
        ]
        return await self._async_process_day_range(day_range)

    async def async_reimport_statistics(self, start_date: date) -> None:
        """Reimport all statistics from *start_date* through yesterday (inclusive)."""
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

