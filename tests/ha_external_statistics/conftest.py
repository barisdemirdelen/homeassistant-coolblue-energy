"""Shared helpers for the external-statistics tests.

These tests run against a real Home Assistant recorder rather than a mock of
one, so they need a way to ask the database what was actually stored.
"""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import (
    StatisticsRow,
    statistics_during_period,
)
from homeassistant.core import HomeAssistant


async def async_stat_rows(
    hass: HomeAssistant,
    stat_id: str,
    start: datetime,
    end: datetime,
) -> list[StatisticsRow]:
    """Return the hourly rows the recorder holds for *stat_id* in ``[start, end)``."""
    result = await get_instance(hass).async_add_executor_job(
        lambda: statistics_during_period(
            hass,
            start,
            end,
            {stat_id},
            "hour",
            None,
            {"sum"},
        ),
    )
    return list(result.get(stat_id, []))
