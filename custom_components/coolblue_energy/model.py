"""
model.py

Pydantic models for the Coolblue Energy API.

getInsights
-----------
Request  → GetMeterReadingsRequest
Response → list[MeterReadingEntry]

The API returns three distinct response shapes depending on ``energy_type``:

* ``"electricity"`` — electricity usage filled in; gas usage = 0; costs may
                      be populated or zero depending on the account/timing.
* ``"gas"``         — gas usage filled in; electricity usage = 0; costs are
                      typically zero (use a dedicated ``"costs"`` call).
* ``"costs"``       — usage = 0 for both; cost fields are populated.

Callers therefore make three separate requests and keep the results in separate
lists.  The response timestamps are Amsterdam local time formatted with a
misleading ``Z`` suffix; :attr:`MeterReadingEntry.name` extracts the hour
verbatim to produce an Amsterdam-local ``"HH:00"`` label used by the
statistics helpers.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

from .util import CoercedFloat

_TZ_NL = ZoneInfo("Europe/Amsterdam")


class CamelCaseModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


# ── Request ───────────────────────────────────────────────────────────────────


class GetMeterReadingsRequest(CamelCaseModel):
    """
    Parameters for the ``getInsights`` Next.js server action.

    Call :meth:`to_payload` to get the ordered list expected by the API.
    """

    customer_id: str
    """Debtor number, e.g. ``"00844083"``."""

    connection_uuid: str
    """Location UUID, e.g. ``"3addb383-a979-40b4-8487-0f3bc0854da5"``."""

    energy_type: Literal["electricity", "gas", "costs"]
    """Which meter to query."""

    for_date: date = Field(default_factory=date.today)
    """Date to fetch hourly data for."""

    cumulative: bool = False
    """Pass ``False`` for hourly interval data."""

    def to_payload(self) -> list:
        """Return the JSON-serialisable positional-argument list for the API."""
        # Next.js serialises Date objects as "$D" + ISO-8601 UTC string.
        # The API expects the start of the requested day in Amsterdam time,
        # expressed as UTC.
        day_start_utc = datetime(
            self.for_date.year,
            self.for_date.month,
            self.for_date.day,
            0,
            0,
            tzinfo=_TZ_NL,
        ).astimezone(timezone.utc)
        next_date = f"$D{day_start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
        return [
            self.customer_id,
            self.connection_uuid,
            self.energy_type,
            False,
            next_date,
            self.for_date.year,
            self.for_date.month,
            self.for_date.day,
            self.cumulative,
        ]


# ── Response sub-models ───────────────────────────────────────────────────────


class PeakUsage(CamelCaseModel):
    """kWh split across peak / off-peak / single tariff bands."""

    peak: CoercedFloat = 0.0
    off_peak: CoercedFloat = 0.0  # API field name: "offPeak"
    single: CoercedFloat = 0.0  # single-tariff meter value
    total: CoercedFloat = 0.0


class AmountData(BaseModel):
    """A monetary amount in EUR, as returned by the API ``cost`` sub-objects."""

    amount: CoercedFloat = 0.0


class ElectricityData(BaseModel):
    """Electricity portion of an API response entry."""

    usage: PeakUsage = Field(default_factory=PeakUsage)
    cost: AmountData = Field(default_factory=AmountData)


class GasData(BaseModel):
    """Gas portion of an API response entry.

    The ``usage`` field may arrive as either a plain number or a dict with a
    ``total`` key; the validator normalises both forms to a float.
    """

    usage: CoercedFloat = 0.0
    """m³ consumed in this hour."""

    cost: AmountData = Field(default_factory=AmountData)

    @model_validator(mode="before")
    @classmethod
    def _coerce_usage(cls, data: Any) -> Any:
        """Flatten ``usage: {"total": x}`` → ``usage: x``."""
        match data:
            case {"usage": dict(u), **rest}:
                return {"usage": u.get("total", 0), **rest}
        return data


class FeedInData(BaseModel):
    """Solar feed-in portion of an API response entry (``feedIn`` key)."""

    production: PeakUsage = Field(default_factory=PeakUsage)
    """kWh fed back to the grid.  Values are **positive** as returned by the API."""

    cost: AmountData = Field(default_factory=AmountData)
    """Feed-in compensation credit; **negative** as returned by the API."""


class SmartDeviceItemUsage(CamelCaseModel):
    """Usage breakdown for one smart-device appliance type."""

    free: CoercedFloat = 0.0
    non_free: CoercedFloat = 0.0  # API field name: "nonFree"
    total: CoercedFloat = 0.0


class SmartDeviceItem(BaseModel):
    """One smart-device appliance's hourly usage."""

    usage: SmartDeviceItemUsage = Field(default_factory=SmartDeviceItemUsage)


class SmartDevicesData(BaseModel):
    """Aggregated smart-device (washing machine / dryer) usage for one hour
    (``smartDevices`` key in the API response)."""

    washing: SmartDeviceItem = Field(default_factory=SmartDeviceItem)
    drying: SmartDeviceItem = Field(default_factory=SmartDeviceItem)
    cost: CoercedFloat = 0.0


# ── Top-level response entry ──────────────────────────────────────────────────


class MeterReadingEntry(CamelCaseModel):
    """
    One hourly entry returned by ``getInsights``, parsed directly from the
    API JSON without any field transformation.

    All three response shapes (electricity, gas, costs) map onto this same
    model; fields not relevant to the requested ``energy_type`` will be
    zero / absent.

    Parse the full response list with::

        from pydantic import TypeAdapter
        entries = TypeAdapter(list[MeterReadingEntry]).validate_python(raw_list)
    """

    timestamp: str
    """ISO-8601 string in Amsterdam local time with a misleading ``Z`` suffix,
    e.g. ``"2026-06-08T14:00:00.000Z"`` means 14:00 Amsterdam, not 14:00 UTC."""

    electricity: ElectricityData = Field(default_factory=ElectricityData)
    """Electricity usage (kWh) and cost breakdown."""

    gas: GasData = Field(default_factory=GasData)
    """Gas usage (m³) and cost breakdown."""

    feed_in: FeedInData | None = None
    """Solar feed-in data; ``None`` when absent from the API response."""

    smart_devices: SmartDevicesData = Field(default_factory=SmartDevicesData)
    """Smart-device usage breakdown (``smartDevices`` in the API)."""

    dynamic_price: CoercedFloat = 0.0
    """Dynamic spot price in EUR/kWh for this hour (``dynamicPrice`` in the API)."""

    @field_validator("electricity", "gas", "smart_devices", mode="before")
    @classmethod
    def _none_to_empty(cls, v: Any) -> Any:
        """Coerce ``null`` API values to empty dicts so defaults apply."""
        return v if v is not None else {}

    @property
    def name(self) -> str:
        """Amsterdam-local hour label derived from :attr:`timestamp`, e.g. ``"14:00"``.

        The API returns timestamps in Amsterdam local time with a misleading
        ``Z`` suffix, so the hour is extracted verbatim from the string.
        """
        return f"{self.timestamp[11:13]}:00"
