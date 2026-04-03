"""Sensor platform for Coolblue Energy."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DEFAULT_NAME, DOMAIN, EUR_KWH
from .coordinator import CoolblueCoordinator, CoordinatorData

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class CoolblueSensorDescription(SensorEntityDescription):
    """Extends SensorEntityDescription with a typed value extractor."""

    value_fn: Callable[[CoordinatorData], float | None]


# ── Sensor catalogue ──────────────────────────────────────────────────────────

_SENSORS: tuple[CoolblueSensorDescription, ...] = (
    # ── Energy / gas (informational — no state_class so the recorder does not
    #    create statistics that would conflict with the external statistics
    #    injected by the coordinator) ──────────────────────────────────────────
    CoolblueSensorDescription(
        key="electricity_consumed",
        translation_key="electricity_consumed",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        suggested_display_precision=3,
        value_fn=lambda d: (
            sum(e.electricity.total for e in d.electricity) if d.electricity else None
        ),
    ),
    CoolblueSensorDescription(
        key="electricity_returned",
        translation_key="electricity_returned",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        suggested_display_precision=3,
        value_fn=lambda d: (
            sum(e.production.total for e in d.electricity) if d.electricity else None
        ),
    ),
    CoolblueSensorDescription(
        key="gas_consumed",
        translation_key="gas_consumed",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        suggested_display_precision=3,
        value_fn=lambda d: sum(e.gas for e in d.gas) if d.gas else None,
    ),
    # ── Informational ─────────────────────────────────────────────────────────
    CoolblueSensorDescription(
        key="spot_price",
        translation_key="spot_price",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=EUR_KWH,
        suggested_display_precision=4,
        # Last non-zero price in the day's entries (most recent settled hour).
        value_fn=lambda d: (
            next((e.price for e in reversed(d.electricity) if e.price), None)
            if d.electricity
            else None
        ),
    ),
    CoolblueSensorDescription(
        key="daily_electricity_cost",
        translation_key="daily_electricity_cost",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=CURRENCY_EURO,
        suggested_display_precision=2,
        value_fn=lambda d: (
            sum(e.costs.electricity.total for e in d.electricity)
            if d.electricity
            else None
        ),
    ),
    CoolblueSensorDescription(
        key="daily_gas_cost",
        translation_key="daily_gas_cost",
        state_class=SensorStateClass.MEASUREMENT,
        native_unit_of_measurement=CURRENCY_EURO,
        suggested_display_precision=2,
        value_fn=lambda d: sum(e.costs.gas.total for e in d.gas) if d.gas else None,
    ),
)


# ── Platform setup ────────────────────────────────────────────────────────────


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Coolblue Energy sensor entities from a config entry."""
    coordinator: CoolblueCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    async_add_entities(
        CoolblueSensor(coordinator, description, entry) for description in _SENSORS
    )


# ── Entity class ──────────────────────────────────────────────────────────────


class CoolblueSensor(CoordinatorEntity[CoolblueCoordinator], SensorEntity):
    """A single Coolblue Energy informational sensor."""

    entity_description: CoolblueSensorDescription
    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: CoolblueCoordinator,
        description: CoolblueSensorDescription,
        entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"
        self._attr_device_info = DeviceInfo(
            entry_type=DeviceEntryType.SERVICE,
            identifiers={(DOMAIN, entry.entry_id)},
            name=DEFAULT_NAME,
            manufacturer="Coolblue",
        )

    @property
    def suggested_object_id(self) -> str:
        """Use the description key as entity object ID, independent of display name."""
        return self.entity_description.key

    @property
    def native_value(self) -> float | None:
        """Return the sensor value derived from yesterday's coordinator data."""
        if self.coordinator.data is None:
            return None
        return self.entity_description.value_fn(self.coordinator.data)
