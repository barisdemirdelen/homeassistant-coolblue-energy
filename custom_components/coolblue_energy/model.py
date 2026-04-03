"""
model.py

Pydantic models for the Coolblue Energy API.

getMeterReadings
----------------
Request  → GetMeterReadingsRequest
Response → list[MeterReadingEntry]
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ── Shared config ─────────────────────────────────────────────────────────────


class CamelCaseModel(BaseModel):
    """Base model that accepts both camelCase aliases and snake_case field names."""

    model_config = ConfigDict(populate_by_name=True)


# ── Request ───────────────────────────────────────────────────────────────────


class GetMeterReadingsRequest(CamelCaseModel):
    """
    Parameters for the ``getMeterReadings`` Next.js server action.

    Call :meth:`to_payload` to get the ordered list expected by the API.
    """

    customer_id: str
    """Debtor number, e.g. ``"00844083"``."""

    connection_uuid: str
    """Location UUID, e.g. ``"3addb383-a979-40b4-8487-0f3bc0854da5"``."""

    energy_type: Literal["electricity", "gas"] = "electricity"
    """Which meter to query."""

    for_date: date = Field(default_factory=date.today)
    """Date to fetch hourly data for."""

    cumulative: bool = False
    """
    Seventh positional argument observed in production traffic.
    Exact semantics are not yet confirmed; pass ``False`` for hourly interval data.
    """

    def to_payload(self) -> list:
        """Return the JSON-serialisable positional-argument list for the API."""
        return [
            self.customer_id,
            self.connection_uuid,
            self.energy_type,
            self.for_date.year,
            self.for_date.month,
            self.for_date.day,
            self.cumulative,
        ]


# ── Response sub-models ───────────────────────────────────────────────────────


class PeakUsage(CamelCaseModel):
    """kWh split across tariff bands (electricity / solar production)."""

    total: float
    off_peak: float = Field(alias="offPeak")
    peak: float


class CostComponent(CamelCaseModel):
    """EUR cost split into fixed, consumption and total for one commodity."""

    total: float
    fixed: float
    consumption: float


class HourlyCosts(CamelCaseModel):
    """Full cost breakdown for one hour."""

    electricity: CostComponent
    gas: CostComponent
    production: float
    """Saleback credit (negative cost, i.e. earnings from feed-in)."""


class SmartDeviceUsage(CamelCaseModel):
    """kWh consumed by smart devices (e.g. smart washer / dryer)."""

    free: float
    paid: float
    has_free_drying: bool = Field(alias="hasFreeDrying")
    has_free_washing: bool = Field(alias="hasFreeWashing")


# ── Top-level response entry ──────────────────────────────────────────────────


class MeterReadingEntry(CamelCaseModel):
    """
    One hourly entry returned by ``getMeterReadings``.

    Parse a raw dict with::

        entry = MeterReadingEntry.model_validate(raw_dict)

    Parse the full response list with::

        from pydantic import TypeAdapter
        entries = TypeAdapter(list[MeterReadingEntry]).validate_python(raw_list)
    """

    name: str
    """Hour label, e.g. ``"00:00"``."""

    electricity: PeakUsage
    """Electricity consumed from the grid, in kWh."""

    production: PeakUsage
    """Electricity fed back to the grid (solar), in kWh."""

    costs: HourlyCosts

    smart_device_usage: SmartDeviceUsage = Field(alias="smartDeviceUsage")

    price: float
    """Spot price in EUR/kWh for this hour."""

    gas: float
    """Gas consumed in m³."""
