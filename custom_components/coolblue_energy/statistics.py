"""
statistics.py

Coolblue Energy external-statistic instances.

Defines the six ``ExternalStatistic`` objects for this integration and the
Amsterdam-timezone helpers needed to convert Coolblue API hour labels
(``"HH:MM"``) to UTC datetimes.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume

from .const import (
    DOMAIN,
)
from .const import (
    STAT_ELECTRICITY_CONSUMED as _ID_ELEC_CONSUMED,
)
from .const import (
    STAT_ELECTRICITY_COST as _ID_ELEC_COST,
)
from .const import (
    STAT_ELECTRICITY_RETURNED as _ID_ELEC_RETURNED,
)
from .const import (
    STAT_ELECTRICITY_RETURNED_COMPENSATION as _ID_ELEC_COMP,
)
from .const import (
    STAT_GAS_CONSUMED as _ID_GAS_CONSUMED,
)
from .const import (
    STAT_GAS_COST as _ID_GAS_COST,
)
from .external_statistic import ExternalStatistic
from .model import MeterReadingEntry

# Dutch local timezone — all hour labels from the API are in this zone.
_TZ_NL = ZoneInfo("Europe/Amsterdam")


# ── Timezone helpers ──────────────────────────────────────────────────────────


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


def _ts(entry: MeterReadingEntry, for_date: date) -> datetime:
    """``period_start_fn`` adapter: delegate to ``_entry_to_utc`` via ``entry.name``."""
    return _entry_to_utc(entry.name, for_date)


# ── Concrete statistic instances ──────────────────────────────────────────────

ELECTRICITY_CONSUMED: ExternalStatistic[MeterReadingEntry] = ExternalStatistic(
    statistic_id=_ID_ELEC_CONSUMED,
    name="Coolblue Electricity Consumed",
    source=DOMAIN,
    unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
    unit_class="energy",
    period_start_fn=_ts,
    value_fn=lambda e: e.electricity.total,
)

ELECTRICITY_RETURNED: ExternalStatistic[MeterReadingEntry] = ExternalStatistic(
    statistic_id=_ID_ELEC_RETURNED,
    name="Coolblue Electricity Returned",
    source=DOMAIN,
    unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
    unit_class="energy",
    period_start_fn=_ts,
    value_fn=lambda e: -e.production.total,
)

GAS_CONSUMED: ExternalStatistic[MeterReadingEntry] = ExternalStatistic(
    statistic_id=_ID_GAS_CONSUMED,
    name="Coolblue Gas Consumed",
    source=DOMAIN,
    unit_of_measurement=UnitOfVolume.CUBIC_METERS,
    unit_class="volume",
    period_start_fn=_ts,
    value_fn=lambda e: e.gas,
)

ELECTRICITY_COST: ExternalStatistic[MeterReadingEntry] = ExternalStatistic(
    statistic_id=_ID_ELEC_COST,
    name="Coolblue Electricity Cost",
    source=DOMAIN,
    unit_of_measurement=CURRENCY_EURO,
    unit_class=None,
    period_start_fn=_ts,
    value_fn=lambda e: e.costs.electricity.total,
)

ELECTRICITY_RETURNED_COMPENSATION: ExternalStatistic[MeterReadingEntry] = (
    ExternalStatistic(
        statistic_id=_ID_ELEC_COMP,
        name="Coolblue Electricity Returned Compensation",
        source=DOMAIN,
        unit_of_measurement=CURRENCY_EURO,
        unit_class=None,
        period_start_fn=_ts,
        value_fn=lambda e: -e.costs.production,
    )
)

GAS_COST: ExternalStatistic[MeterReadingEntry] = ExternalStatistic(
    statistic_id=_ID_GAS_COST,
    name="Coolblue Gas Cost",
    source=DOMAIN,
    unit_of_measurement=CURRENCY_EURO,
    unit_class=None,
    period_start_fn=_ts,
    value_fn=lambda e: e.costs.gas.total,
)

ALL_STATISTICS: tuple[ExternalStatistic[MeterReadingEntry], ...] = (
    ELECTRICITY_CONSUMED,
    ELECTRICITY_RETURNED,
    GAS_CONSUMED,
    ELECTRICITY_COST,
    ELECTRICITY_RETURNED_COMPENSATION,
    GAS_COST,
)
