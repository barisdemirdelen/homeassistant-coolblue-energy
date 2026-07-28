"""Tests for ha_external_statistic.recorder helpers.

These run against a real Home Assistant recorder. Prior statistics are written
with ``ExternalStatistic.inject`` and read back through the helpers under test,
so a seed sum the code claims to have fetched is one the database really held.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch

import pytest
from homeassistant.components.recorder.statistics import StatisticMeanType
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.coolblue_energy.ha_external_statistics import (
    recorder as _recorder_mod,
)
from custom_components.coolblue_energy.ha_external_statistics.external_statistic import (
    ExternalStatistic,
)
from custom_components.coolblue_energy.ha_external_statistics.recorder import (
    async_get_last_sum,
    async_inject_day,
)

from .conftest import async_stat_rows

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@dataclass
class Entry:
    hour: int
    value: float


_DATE = date(2024, 3, 15)
_DAY_START = datetime(2024, 3, 15, 0, 0, tzinfo=UTC)
_PRIOR = date(2024, 3, 14)


def _ts(entry: Entry, for_date: date) -> datetime:
    return datetime(
        for_date.year, for_date.month, for_date.day, entry.hour, 0, tzinfo=UTC
    )


def _make_stat(stat_id: str = "dom:stat", **kwargs) -> ExternalStatistic[Entry]:
    defaults: dict = {
        "statistic_id": stat_id,
        "name": "Test",
        "source": "dom",
        "unit_of_measurement": "kWh",
        "unit_class": "energy",
        "period_start_fn": _ts,
        "value_fn": lambda e: e.value,
    }
    defaults.update(kwargs)
    return ExternalStatistic[Entry](**defaults)


async def _store(
    hass: HomeAssistant,
    stat: ExternalStatistic[Entry],
    entries: list[Entry],
    for_date: date = _PRIOR,
    seed_sum: float = 0.0,
) -> None:
    """Write statistics into the recorder and wait for them to land."""
    stat.inject(hass, entries, for_date, seed_sum)
    await async_wait_recording_done(hass)


# ---------------------------------------------------------------------------
# async_get_last_sum
# ---------------------------------------------------------------------------


class TestAsyncGetLastSum:
    async def test_returns_last_sum(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        stat = _make_stat()
        await _store(hass, stat, [Entry(21, 10.0), Entry(22, 10.0), Entry(23, 10.0)])

        result = await async_get_last_sum(hass, "dom:stat", _DAY_START)

        assert result == pytest.approx(30.0)

    async def test_returns_zero_when_no_prior_stats(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        result = await async_get_last_sum(hass, "dom:stat", _DAY_START)

        assert result == pytest.approx(0.0)

    async def test_returns_zero_when_statistic_has_no_sum(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        """A mean-only statistic stores rows without a sum; that must read as 0.0."""
        stat = _make_stat(
            "dom:mean_only",
            has_sum=False,
            mean_type=StatisticMeanType.ARITHMETIC,
            mean_fn=lambda e: e.value,
        )
        await _store(hass, stat, [Entry(23, 5.0)])

        result = await async_get_last_sum(hass, "dom:mean_only", _DAY_START)

        assert result == pytest.approx(0.0)

    async def test_query_window_uses_lookback_hours(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        """lookback_hours sets the lower bound; before_dt is an exclusive upper one."""
        stat = _make_stat()
        # 14:00 the day before is exactly 10 h before the 00:00 bound, 13:00 is 11 h
        # before it, and the 00:00 row sits on the bound itself.
        await _store(hass, stat, [Entry(13, 1.0), Entry(14, 2.0)])
        await _store(hass, stat, [Entry(0, 99.0)], _DATE, seed_sum=3.0)

        within = await async_get_last_sum(
            hass, "dom:stat", _DAY_START, lookback_hours=10
        )
        outside = await async_get_last_sum(
            hass, "dom:stat", _DAY_START, lookback_hours=9
        )

        assert within == pytest.approx(3.0)  # the 14:00 row, not the 00:00 one
        assert outside == pytest.approx(0.0)

    async def test_default_lookback_is_25_hours(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        stat = _make_stat()
        # 23:00 two days earlier is exactly 25 h before the bound.
        await _store(hass, stat, [Entry(23, 4.0)], _DATE - timedelta(days=2))

        default = await async_get_last_sum(hass, "dom:stat", _DAY_START)
        narrower = await async_get_last_sum(
            hass, "dom:stat", _DAY_START, lookback_hours=24
        )

        assert default == pytest.approx(4.0)
        assert narrower == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# async_inject_day
# ---------------------------------------------------------------------------


class TestAsyncInjectDay:
    async def test_uses_provided_seed_sums(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        """A supplied seed is used as-is; the database is never consulted."""
        stat = _make_stat("dom:a")
        # If the DB were read, the seed would be 1000.0 rather than the given 100.0.
        await _store(hass, stat, [Entry(23, 1000.0)])

        result = await async_inject_day(
            hass, [(stat, [Entry(0, 5.0)])], _DATE, _DAY_START, {"dom:a": 100.0}
        )

        assert result["dom:a"] == pytest.approx(105.0)

    async def test_queries_db_when_seed_missing(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        stat = _make_stat("dom:a")
        await _store(hass, stat, [Entry(23, 50.0)])

        result = await async_inject_day(
            hass, [(stat, [Entry(0, 3.0)])], _DATE, _DAY_START, None
        )

        assert result["dom:a"] == pytest.approx(53.0)

    async def test_multiple_stats_chained(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        stat_a = _make_stat("dom:a")
        stat_b = _make_stat("dom:b")

        result = await async_inject_day(
            hass,
            [(stat_a, [Entry(0, 1.0)]), (stat_b, [Entry(0, 2.0)])],
            _DATE,
            _DAY_START,
            {"dom:a": 10.0, "dom:b": 20.0},
        )

        assert result["dom:a"] == pytest.approx(11.0)
        assert result["dom:b"] == pytest.approx(22.0)

    async def test_empty_entries_still_returns_seed(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        """Empty entry list: nothing is stored but the seed is kept for chaining."""
        stat = _make_stat("dom:a")

        result = await async_inject_day(
            hass, [(stat, [])], _DATE, _DAY_START, {"dom:a": 42.0}
        )
        await async_wait_recording_done(hass)

        assert result["dom:a"] == pytest.approx(42.0)
        assert (
            await async_stat_rows(
                hass,
                "dom:a",
                _DAY_START - timedelta(days=2),
                _DAY_START + timedelta(days=1),
            )
            == []
        )

    async def test_partial_seed_sums_queries_db_for_missing(
        self, recorder_mock: None, hass: HomeAssistant
    ) -> None:
        """Only stats absent from seed_sums fall back to the database."""
        stat_a = _make_stat("dom:a")
        stat_b = _make_stat("dom:b")
        # dom:a is seeded, so its stored 1000.0 must be ignored; dom:b must be read.
        await _store(hass, stat_a, [Entry(23, 1000.0)])
        await _store(hass, stat_b, [Entry(23, 7.0)])

        # Count seed lookups without suppressing them: the real query still runs.
        with patch.object(
            _recorder_mod,
            "async_get_last_sum",
            wraps=_recorder_mod.async_get_last_sum,
        ) as seed_lookup:
            result = await async_inject_day(
                hass,
                [(stat_a, [Entry(0, 1.0)]), (stat_b, [Entry(0, 2.0)])],
                _DATE,
                _DAY_START,
                seed_sums={"dom:a": 5.0},  # dom:b absent -> DB lookup
            )

        assert result["dom:a"] == pytest.approx(6.0)
        assert result["dom:b"] == pytest.approx(9.0)
        seed_lookup.assert_called_once()
        assert seed_lookup.call_args.args[1] == "dom:b"
