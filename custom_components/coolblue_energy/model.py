"""
model.py

Pydantic models for the Coolblue Energy API.

getInsights
-----------
Request  → GetMeterReadingsRequest
Response → list[MeterReadingEntry]

The API returns three distinct response shapes depending on ``energy_type``:

* ``"electricity"`` — electricity usage filled in; gas usage = 0; costs = 0.
* ``"gas"``         — gas usage filled in; electricity usage = 0; costs = 0.
* ``"costs"``       — usage = 0 for both; cost fields are populated.

Callers therefore make three separate requests and keep the results in separate
lists.  The response timestamps are Amsterdam local time formatted with a
misleading ``Z`` suffix; the hour is extracted verbatim to produce an
Amsterdam-local ``"HH:00"`` label used by the statistics helpers.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator
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
            self.for_date.year, self.for_date.month, self.for_date.day,
            0, 0, tzinfo=_TZ_NL,
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
    """kWh split across peak / off-peak tariff bands (or production totals)."""

    peak: CoercedFloat
    off_peak: CoercedFloat  # API field name: "offPeak"
    total: CoercedFloat


class SmartDeviceUsage(BaseModel):
    """Smart-device (washing machine / dryer) usage metadata."""

    free: CoercedFloat = 0.0
    paid: CoercedFloat = 0.0
    has_free_drying: bool = False
    has_free_washing: bool = False


class CostComponent(BaseModel):
    """A cost amount with an optional fixed / variable breakdown (EUR)."""

    total: CoercedFloat
    fixed: CoercedFloat = 0.0
    consumption: CoercedFloat = 0.0


class HourlyCosts(BaseModel):
    """Cost breakdown for one hour: electricity charges, gas charges, and
    the solar feed-in credit (stored as a negative number, matching the API
    ``feedIn.cost.amount`` sign convention)."""

    electricity: CostComponent = Field(default_factory=lambda: CostComponent(total=0.0))
    gas: CostComponent = Field(default_factory=lambda: CostComponent(total=0.0))
    production: CoercedFloat = 0.0  # negative → feed-in credit


# ── Top-level response entry ──────────────────────────────────────────────────


class MeterReadingEntry(BaseModel):
    """
    One hourly entry returned by ``getInsights``.

    The ``name`` field is an Amsterdam-local ``"HH:00"`` label derived from
    the response ``timestamp``.  All three response shapes (electricity, gas,
    costs) are parsed into this same model; fields not relevant to the
    requested ``energy_type`` will be zero.

    Production values (``production.total``, ``costs.production``) are stored
    with **inverted sign** relative to the API so that the statistics
    value functions can uniformly negate them back to positive kWh/EUR.

    Parse the full response list with::

        from pydantic import TypeAdapter
        entries = TypeAdapter(list[MeterReadingEntry]).validate_python(raw_list)
    """

    name: str
    """Amsterdam-local hour label, e.g. ``"14:00"``."""

    electricity: PeakUsage
    """kWh consumed from the grid (non-zero in electricity-type responses)."""

    production: PeakUsage
    """kWh fed back to the grid, stored **negated** (non-zero in electricity
    responses that have solar activity)."""

    costs: HourlyCosts = Field(default_factory=HourlyCosts)
    """EUR cost breakdown (non-zero in costs-type responses)."""

    smart_device_usage: SmartDeviceUsage = Field(default_factory=SmartDeviceUsage)

    price: CoercedFloat = 0.0
    """Dynamic spot price in EUR/kWh for this hour."""

    gas: CoercedFloat = 0.0
    """m³ of gas consumed (non-zero in gas-type responses)."""

    @model_validator(mode="before")
    @classmethod
    def _from_api_response(cls, data: object) -> object:
        """
        Transform the raw ``getInsights`` JSON dict into domain-model fields.

        The validator is a no-op when the data already uses domain-model keys
        (i.e. when created directly in tests via keyword arguments).

        API timestamp quirk
        -------------------
        The API returns timestamps in **Amsterdam local time** with a
        misleading ``Z`` suffix (e.g. ``"2026-06-08T14:00:00.000Z"`` means
        14:00 Amsterdam, not 14:00 UTC).  We therefore extract the hour
        directly from the string without any timezone conversion.
        """
        if not isinstance(data, dict) or "name" in data:
            return data
        if "timestamp" not in data:
            return data

        ts_str: str = data["timestamp"]  # e.g. "2026-06-08T14:00:00.000Z"
        hour = ts_str[11:13]             # "14"
        name = f"{hour}:00"

        # ── Electricity usage ──────────────────────────────────────────────
        elec = data.get("electricity") or {}
        elec_usage = elec.get("usage") or {}
        elec_cost_amount = (elec.get("cost") or {}).get("amount", 0)

        # ── Gas usage (scalar, not a nested dict) ─────────────────────────
        gas_raw = data.get("gas") or {}
        gas_usage = gas_raw.get("usage", 0)
        if isinstance(gas_usage, dict):
            gas_usage = gas_usage.get("total", 0)
        gas_cost_amount = (gas_raw.get("cost") or {}).get("amount", 0)

        # ── Solar feed-in ─────────────────────────────────────────────────
        feed_in = data.get("feedIn")
        if feed_in:
            prod_raw = feed_in.get("production") or {}
            # Store production as **negative** so value_fn = -e.production.total
            # gives the positive kWh-returned figure.
            prod_peak     = -(prod_raw.get("peak", 0) or 0)
            prod_off_peak = -(prod_raw.get("offPeak", 0) or 0)
            prod_total    = -(prod_raw.get("total", 0) or 0)
            # feedIn.cost.amount is already negative (cost credit) — keep sign.
            prod_cost = (feed_in.get("cost") or {}).get("amount", 0)
        else:
            prod_peak = prod_off_peak = prod_total = 0.0
            prod_cost = 0.0

        return {
            "name": name,
            "electricity": {
                "peak":     elec_usage.get("peak", 0),
                "off_peak": elec_usage.get("offPeak", 0),
                "total":    elec_usage.get("total", 0),
            },
            "production": {
                "peak":     prod_peak,
                "off_peak": prod_off_peak,
                "total":    prod_total,
            },
            "costs": {
                "electricity": {"total": elec_cost_amount},
                "gas":         {"total": gas_cost_amount},
                "production":  prod_cost,
            },
            "smart_device_usage": {},
            "price":              data.get("dynamicPrice", 0),
            "gas":                gas_usage,
        }
