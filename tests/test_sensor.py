"""
test_sensor.py

Tests for sensor entity native_value calculations.
All sensor math is a pure function of CoordinatorData so no HA wiring is needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from custom_components.coolblue_energy.coordinator import CoordinatorData
from custom_components.coolblue_energy.sensor import _SENSORS, CoolblueSensor

from .conftest import make_day_costs, make_day_electricity, make_day_gas


def _make_sensor(key: str, electricity=None, gas=None, costs=None) -> CoolblueSensor:
    """Instantiate a sensor entity bypassing HA entity registry."""
    desc = next(d for d in _SENSORS if d.key == key)
    sensor = object.__new__(CoolblueSensor)
    sensor.entity_description = desc
    sensor.coordinator = MagicMock()
    sensor.coordinator.data = CoordinatorData(
        electricity=electricity if electricity is not None else make_day_electricity(),
        gas=gas if gas is not None else make_day_gas(),
        costs=costs if costs is not None else make_day_costs(),
    )
    return sensor


class TestNativeValue:
    """
    Expected values for 24 uniform entries
    (make_day_elec: 1.0 kWh elec, 0.2 kWh production, €0.25 price, €0.25 elec cost;
     make_day_gas:  0.05 m³ gas, €0.10 gas cost)
    """

    def test_electricity_consumed(self):
        """24 h × 1.0 kWh = 24.0 kWh."""
        assert _make_sensor("electricity_consumed").native_value == pytest.approx(24.0)

    def test_electricity_returned(self):
        """24 h × 0.2 kWh = 4.8 kWh."""
        assert _make_sensor("electricity_returned").native_value == pytest.approx(4.8)

    def test_gas_consumed(self):
        """24 h × 0.05 m³ = 1.2 m³."""
        assert _make_sensor("gas_consumed").native_value == pytest.approx(1.2)

    def test_daily_electricity_cost(self):
        """24 cost entries × €0.25 = €6.00 (from costs request, includes fixed fee)."""
        assert _make_sensor("daily_electricity_cost").native_value == pytest.approx(6.0)

    def test_daily_gas_cost(self):
        """24 cost entries × €0.10 = €2.40 (from costs request, includes fixed fee)."""
        assert _make_sensor("daily_gas_cost").native_value == pytest.approx(2.4)

    def test_daily_electricity_cost_empty_costs(self):
        """Empty costs list must return None, not 0.0."""
        assert _make_sensor("daily_electricity_cost", costs=[]).native_value is None

    def test_daily_gas_cost_empty_costs(self):
        """Empty costs list must return None, not 0.0."""
        assert _make_sensor("daily_gas_cost", costs=[]).native_value is None

    def test_all_sensors_return_none_when_data_is_none(self):
        """All sensors must handle coordinator.data = None without raising."""
        for desc in _SENSORS:
            sensor = object.__new__(CoolblueSensor)
            sensor.entity_description = desc
            sensor.coordinator = MagicMock()
            sensor.coordinator.data = None
            assert sensor.native_value is None, (
                f"Sensor '{desc.key}' returned {sensor.native_value!r} instead of None"
            )

    def test_electricity_consumed_empty_elec(self):
        """Empty elec list must return None, not 0.0 or raise."""
        assert _make_sensor("electricity_consumed", electricity=[]).native_value is None

    def test_gas_consumed_empty_gas(self):
        """Empty gas list must return None, not 0.0 or raise."""
        assert _make_sensor("gas_consumed", gas=[]).native_value is None

    def test_electricity_returned_empty_elec(self):
        assert _make_sensor("electricity_returned", electricity=[]).native_value is None

    def test_sensor_unique_ids_are_distinct(self):
        """Each sensor description must have a distinct key (used for unique_id)."""
        keys = [d.key for d in _SENSORS]
        assert len(keys) == len(set(keys))

    def test_all_sensor_keys_have_translation_key(self):
        """Every sensor must declare a translation_key for HA entity naming."""
        for desc in _SENSORS:
            assert desc.translation_key is not None, (
                f"Sensor '{desc.key}' is missing translation_key"
            )
