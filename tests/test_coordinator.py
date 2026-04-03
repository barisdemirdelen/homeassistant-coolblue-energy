"""
test_coordinator.py

Tests for:
  - _day_start_utc / _entry_to_utc   (timezone helpers)
  - CoolblueCoordinator._get_sum_before
  - CoolblueCoordinator._inject_statistics
  - CoolblueCoordinator._async_backfill
  - CoolblueCoordinator._async_update_data

StatisticData and StatisticMetaData are TypedDicts (plain dicts with typed
keys), so dict-style access is used throughout.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from custom_components.coolblue_energy.const import (
    BACKFILL_DAYS,
    STAT_ELECTRICITY_CONSUMED,
    STAT_ELECTRICITY_RETURNED,
    STAT_GAS_CONSUMED,
)
from custom_components.coolblue_energy.coordinator import (
    CoordinatorData,
    _day_start_utc,
    _entry_to_utc,
)

_STATS_PATH = "custom_components.coolblue_energy.coordinator.statistics_during_period"
_ADD_PATH = (
    "custom_components.coolblue_energy.coordinator.async_add_external_statistics"
)

_ALL_STATS = (STAT_ELECTRICITY_CONSUMED, STAT_ELECTRICITY_RETURNED, STAT_GAS_CONSUMED)
_EMPTY_SEED = {s: 0.0 for s in _ALL_STATS}


# ── Timezone helpers ──────────────────────────────────────────────────────────


class TestEntryToUtc:
    """_entry_to_utc converts 'HH:MM' Amsterdam-local to UTC."""

    def test_winter_cet_offset(self):
        """CET = UTC+1: 14:00 Amsterdam on Jan 15 → 13:00 UTC."""
        assert _entry_to_utc("14:00", date(2026, 1, 15)) == datetime(
            2026, 1, 15, 13, 0, tzinfo=timezone.utc
        )

    def test_summer_cest_offset(self):
        """CEST = UTC+2: 14:00 Amsterdam on Jul 15 → 12:00 UTC."""
        assert _entry_to_utc("14:00", date(2026, 7, 15)) == datetime(
            2026, 7, 15, 12, 0, tzinfo=timezone.utc
        )

    def test_midnight_winter_crosses_day_boundary(self):
        """00:00 Amsterdam CET → 23:00 of the previous UTC day."""
        assert _entry_to_utc("00:00", date(2026, 1, 15)) == datetime(
            2026, 1, 14, 23, 0, tzinfo=timezone.utc
        )

    def test_midnight_summer_crosses_day_boundary(self):
        """00:00 Amsterdam CEST → 22:00 of the previous UTC day."""
        assert _entry_to_utc("00:00", date(2026, 7, 15)) == datetime(
            2026, 7, 14, 22, 0, tzinfo=timezone.utc
        )

    def test_result_is_utc(self):
        result = _entry_to_utc("10:00", date(2026, 3, 1))
        assert result.tzinfo is timezone.utc

    def test_hours_across_24h_day(self):
        """Consecutive hours must be exactly 1 h apart in UTC."""
        day = date(2026, 1, 20)
        times = [_entry_to_utc(f"{h:02d}:00", day) for h in range(24)]
        diffs = [times[i + 1] - times[i] for i in range(23)]
        assert all(d == timedelta(hours=1) for d in diffs)


class TestDayStartUtc:
    """_day_start_utc converts a date's Amsterdam midnight to UTC."""

    def test_winter(self):
        """Jan 15 00:00 CET → Jan 14 23:00 UTC."""
        assert _day_start_utc(date(2026, 1, 15)) == datetime(
            2026, 1, 14, 23, 0, tzinfo=timezone.utc
        )

    def test_summer(self):
        """Jul 15 00:00 CEST → Jul 14 22:00 UTC."""
        assert _day_start_utc(date(2026, 7, 15)) == datetime(
            2026, 7, 14, 22, 0, tzinfo=timezone.utc
        )

    def test_equals_first_entry_utc(self):
        """_day_start_utc must equal _entry_to_utc('00:00', day)."""
        for day in [date(2026, 1, 15), date(2026, 7, 15)]:
            assert _day_start_utc(day) == _entry_to_utc("00:00", day)

    def test_dst_spring_forward_2026(self):
        """2026 spring forward is March 29 (last Sunday of March).
        Amsterdam 00:00 is still CET on that day → UTC-1h = 23:00 March 28."""
        assert _day_start_utc(date(2026, 3, 29)) == datetime(
            2026, 3, 28, 23, 0, tzinfo=timezone.utc
        )


# ── _get_sum_before ───────────────────────────────────────────────────────────


class TestGetSumBefore:
    async def test_returns_zero_when_no_data(self, coordinator):
        before_dt = datetime(2026, 1, 14, 23, 0, tzinfo=timezone.utc)
        with patch(_STATS_PATH, return_value={}):
            result = await coordinator._get_sum_before(
                STAT_ELECTRICITY_CONSUMED, before_dt
            )
        assert result == 0.0

    async def test_returns_last_entry_sum(self, coordinator):
        before_dt = datetime(2026, 1, 14, 23, 0, tzinfo=timezone.utc)
        fake = {
            STAT_ELECTRICITY_CONSUMED: [
                {"sum": 100.0},
                {"sum": 101.5},  # ← last entry wins
            ]
        }
        with patch(_STATS_PATH, return_value=fake):
            result = await coordinator._get_sum_before(
                STAT_ELECTRICITY_CONSUMED, before_dt
            )
        assert result == pytest.approx(101.5)

    async def test_returns_zero_for_none_sum(self, coordinator):
        """A None sum (data gap) must fall back to 0.0."""
        before_dt = datetime(2026, 1, 14, 23, 0, tzinfo=timezone.utc)
        with patch(
            _STATS_PATH, return_value={STAT_ELECTRICITY_CONSUMED: [{"sum": None}]}
        ):
            result = await coordinator._get_sum_before(
                STAT_ELECTRICITY_CONSUMED, before_dt
            )
        assert result == 0.0

    async def test_queries_25h_window_before_dt(self, coordinator):
        """The query window must start 25 h before before_dt to cover DST days."""
        before_dt = datetime(2026, 1, 14, 23, 0, tzinfo=timezone.utc)
        expected_start = before_dt - timedelta(hours=25)
        captured = {}

        def capture(hass, start, end, stat_ids, period, units, types):
            captured["start"] = start
            return {}

        with patch(_STATS_PATH, side_effect=capture):
            await coordinator._get_sum_before(STAT_ELECTRICITY_CONSUMED, before_dt)

        assert captured["start"] == expected_start


# ── _inject_statistics ────────────────────────────────────────────────────────


class TestInjectStatistics:
    async def test_returns_correct_end_sums(self, coordinator, fake_elec, fake_gas):
        """End-of-day sums = seed + (24 h × hourly delta)."""
        seed = {
            STAT_ELECTRICITY_CONSUMED: 1000.0,
            STAT_ELECTRICITY_RETURNED: 200.0,
            STAT_GAS_CONSUMED: 50.0,
        }
        with patch(_ADD_PATH):
            result = await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=seed
            )

        assert result[STAT_ELECTRICITY_CONSUMED] == pytest.approx(1024.0)  # +24×1.0
        assert result[STAT_ELECTRICITY_RETURNED] == pytest.approx(204.8)  # +24×0.2
        assert result[STAT_GAS_CONSUMED] == pytest.approx(51.2)  # +24×0.05

    async def test_queries_db_when_seed_is_none(self, coordinator, fake_elec, fake_gas):
        """seed_sums=None must trigger a _get_sum_before call for each stat."""
        queried = []
        coordinator._get_sum_before = AsyncMock(
            side_effect=lambda stat_id, _dt: queried.append(stat_id) or 0.0
        )
        with patch(_ADD_PATH):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=None
            )

        assert set(queried) == set(_ALL_STATS)

    async def test_does_not_query_db_when_seed_provided(
        self, coordinator, fake_elec, fake_gas
    ):
        """When a valid seed_sums dict is provided, the DB must not be queried."""
        coordinator._get_sum_before = AsyncMock(return_value=0.0)
        with patch(_ADD_PATH):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )

        coordinator._get_sum_before.assert_not_called()

    async def test_statistic_data_timestamps_are_utc(
        self, coordinator, fake_elec, fake_gas
    ):
        """First entry (00:00 Amsterdam on 2026-01-14) must be 2026-01-13 23:00 UTC."""
        captured = {}

        def capture(hass, metadata, stat_data):
            if metadata["statistic_id"] == STAT_ELECTRICITY_CONSUMED:
                captured["first_start"] = stat_data[0]["start"]

        with patch(_ADD_PATH, side_effect=capture):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )

        assert captured["first_start"] == datetime(
            2026, 1, 13, 23, 0, tzinfo=timezone.utc
        )

    async def test_statistic_data_sums_accumulate(
        self, coordinator, fake_elec, fake_gas
    ):
        """state = hourly delta; sum = running total. Checked for first 3 hours."""
        captured_elec = []

        def capture(hass, metadata, stat_data):
            if metadata["statistic_id"] == STAT_ELECTRICITY_CONSUMED:
                captured_elec.extend(stat_data)

        with patch(_ADD_PATH, side_effect=capture):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )

        assert captured_elec[0]["state"] == pytest.approx(1.0)  # hour 0 delta
        assert captured_elec[0]["sum"] == pytest.approx(1.0)  # running total
        assert captured_elec[1]["state"] == pytest.approx(1.0)
        assert captured_elec[1]["sum"] == pytest.approx(2.0)
        assert captured_elec[23]["sum"] == pytest.approx(24.0)  # full day

    async def test_seed_carried_into_sum(self, coordinator, fake_elec, fake_gas):
        """A non-zero seed must offset all sums in the day's StatisticData."""
        seed = {**_EMPTY_SEED, STAT_ELECTRICITY_CONSUMED: 500.0}
        captured_sums = []

        def capture(hass, metadata, stat_data):
            if metadata["statistic_id"] == STAT_ELECTRICITY_CONSUMED:
                captured_sums.extend(e["sum"] for e in stat_data)

        with patch(_ADD_PATH, side_effect=capture):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=seed
            )

        assert captured_sums[0] == pytest.approx(501.0)  # 500 + 1 kWh
        assert captured_sums[-1] == pytest.approx(524.0)  # 500 + 24 kWh

    async def test_calls_async_add_for_all_three_stats(
        self, coordinator, fake_elec, fake_gas
    ):
        called_ids = []

        def capture(hass, metadata, stat_data):
            called_ids.append(metadata["statistic_id"])

        with patch(_ADD_PATH, side_effect=capture):
            await coordinator._inject_statistics(
                fake_elec, fake_gas, date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )

        assert set(called_ids) == set(_ALL_STATS)

    async def test_no_call_for_empty_entries(self, coordinator):
        """Empty entry lists must produce no async_add_external_statistics call."""
        with patch(_ADD_PATH) as mock_add:
            await coordinator._inject_statistics(
                [], [], date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )
        mock_add.assert_not_called()

    async def test_gas_stat_uses_entry_gas_field(self, coordinator, fake_gas):
        """STAT_GAS_CONSUMED must read entry.gas, not entry.electricity.total."""
        captured = {}

        def capture(hass, metadata, stat_data):
            if metadata["statistic_id"] == STAT_GAS_CONSUMED:
                captured["first_state"] = stat_data[0]["state"]

        with patch(_ADD_PATH, side_effect=capture):
            await coordinator._inject_statistics(
                [], fake_gas, date(2026, 1, 14), seed_sums=_EMPTY_SEED
            )

        assert captured["first_state"] == pytest.approx(0.05)


# ── _async_backfill ───────────────────────────────────────────────────────────


class TestAsyncBackfill:
    async def test_fetches_elec_and_gas_for_each_day(self, coordinator):
        """get_hourly_energy must be called twice per day (elec + gas)."""
        with patch(_STATS_PATH, return_value={}), patch(_ADD_PATH):
            await coordinator._async_backfill(BACKFILL_DAYS)

        assert coordinator._client.get_hourly_energy.call_count == BACKFILL_DAYS * 2

    async def test_returns_yesterday_coordinator_data(
        self, coordinator, fake_elec, fake_gas
    ):
        """The returned CoordinatorData must contain yesterday's entries."""
        with patch(_STATS_PATH, return_value={}), patch(_ADD_PATH):
            result = await coordinator._async_backfill(BACKFILL_DAYS)

        assert isinstance(result, CoordinatorData)
        assert result.electricity == fake_elec
        assert result.gas == fake_gas

    async def test_seeds_are_chained_across_days(self, coordinator):
        """
        On a clean DB (no prior stats), each day must start from the previous
        day's end sum without re-querying the DB.
        Only the initial 3 _get_sum_before calls (one per stat) are expected.
        """
        get_sum_calls = []
        coordinator._get_sum_before = AsyncMock(
            side_effect=lambda stat_id, dt: get_sum_calls.append(stat_id) or 0.0
        )
        with patch(_ADD_PATH):
            await coordinator._async_backfill(3)

        # 3 initial seeds, no more (seeds chained for days 2 and 3)
        assert len(get_sum_calls) == 3

    async def test_failed_day_causes_db_requery_for_next(self, coordinator):
        """
        A day that raises an exception must reset seed_sums to None so the
        following day re-seeds from the DB (3 extra _get_sum_before calls).
        """
        get_sum_calls = []

        async def spy_get_sum(stat_id, dt):
            get_sum_calls.append(stat_id)
            return 0.0

        coordinator._get_sum_before = spy_get_sum

        # Fail the 5th day from today (offset=5 in a 7-day backfill)
        original_fetch = coordinator._fetch_day

        async def patched_fetch(day):
            if (date.today() - day).days == 5:
                raise RuntimeError("Simulated API failure")
            return await original_fetch(day)

        coordinator._fetch_day = patched_fetch

        with patch(_ADD_PATH):
            await coordinator._async_backfill(7)

        # 3 initial + 3 after the failure = at least 6
        assert len(get_sum_calls) >= 6

    async def test_failed_day_does_not_abort_remaining_days(
        self, coordinator, fake_elec, fake_gas
    ):
        """All days after a failure must still be processed."""
        original_fetch = coordinator._fetch_day
        fetch_count = [0]

        async def patched_fetch(day):
            fetch_count[0] += 1
            if (date.today() - day).days == 4:
                raise RuntimeError("Simulated failure")
            return await original_fetch(day)

        coordinator._fetch_day = patched_fetch

        with patch(_STATS_PATH, return_value={}), patch(_ADD_PATH):
            await coordinator._async_backfill(7)

        # All 7 days attempted (even though day-4 failed)
        assert fetch_count[0] == 7


# ── _async_update_data ────────────────────────────────────────────────────────


class TestAsyncUpdateData:
    async def test_first_run_triggers_backfill(self, coordinator, fake_elec, fake_gas):
        """On first call (_backfilled=False), backfill is run and flag is set."""
        backfill_calls = []

        async def mock_backfill(days):
            backfill_calls.append(days)
            return CoordinatorData(electricity=fake_elec, gas=fake_gas)

        coordinator._async_backfill = mock_backfill

        result = await coordinator._async_update_data()

        assert len(backfill_calls) == 1
        assert coordinator._backfilled is True
        assert result.electricity is fake_elec

    async def test_subsequent_run_skips_backfill(
        self, coordinator, fake_elec, fake_gas
    ):
        """With _backfilled=True, backfill must NOT run again."""
        coordinator._backfilled = True
        backfill_calls = []

        async def mock_backfill(days):
            backfill_calls.append(days)
            return CoordinatorData(electricity=[], gas=[])

        coordinator._async_backfill = mock_backfill

        with patch(_STATS_PATH, return_value={}), patch(_ADD_PATH):
            result = await coordinator._async_update_data()

        assert backfill_calls == []
        assert result.electricity == fake_elec

    async def test_wraps_exception_in_update_failed(self, coordinator):
        """Any unhandled exception from the API must be re-raised as UpdateFailed."""
        from homeassistant.helpers.update_coordinator import UpdateFailed

        # Use the normal daily-refresh path (_backfilled=True) so the error
        # propagates directly; the backfill path swallows per-day errors.
        coordinator._backfilled = True
        coordinator._client.get_hourly_energy.side_effect = RuntimeError("API down")

        with pytest.raises(UpdateFailed, match="API down"):
            await coordinator._async_update_data()
