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

from .conftest import make_day_elec, make_day_gas, make_elec_entry


def _make_sensor(key: str, electricity=None, gas=None) -> CoolblueSensor:
    """Instantiate a sensor entity bypassing HA entity registry."""
    desc = next(d for d in _SENSORS if d.key == key)
    sensor = object.__new__(CoolblueSensor)
    sensor.entity_description = desc
    sensor.coordinator = MagicMock()
    sensor.coordinator.data = CoordinatorData(
        electricity=electricity if electricity is not None else make_day_elec(),
        gas=gas if gas is not None else make_day_gas(),
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

    def test_spot_price_last_nonzero(self):
        """Returns last non-zero price from the electricity entries."""
        assert _make_sensor("spot_price").native_value == pytest.approx(0.25)

    def test_daily_electricity_cost(self):
        """24 h × €0.25 = €6.00."""
        assert _make_sensor("daily_electricity_cost").native_value == pytest.approx(6.0)

    def test_daily_gas_cost(self):
        """24 gas entries × €0.10 = €2.40."""
        assert _make_sensor("daily_gas_cost").native_value == pytest.approx(2.4)

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

    def test_spot_price_all_zero_prices(self):
        """When every price is 0.0 (falsy), spot_price returns None."""
        entries = [make_elec_entry(h, price=0.0) for h in range(24)]
        assert _make_sensor("spot_price", electricity=entries).native_value is None

    def test_spot_price_picks_last_nonzero(self):
        """If only the last hour has a non-zero price, that value is returned."""
        entries = [make_elec_entry(h, price=0.0) for h in range(23)]
        entries.append(make_elec_entry(23, price=0.42))
        assert _make_sensor(
            "spot_price", electricity=entries
        ).native_value == pytest.approx(0.42)

    def test_spot_price_ignores_trailing_zeros(self):
        """If the last hours have price=0.0, the latest *non-zero* price is used."""
        entries = [make_elec_entry(h, price=0.30) for h in range(20)]
        entries += [make_elec_entry(h, price=0.0) for h in range(20, 24)]
        # reversed scan hits hour 19 (price=0.30) first
        assert _make_sensor(
            "spot_price", electricity=entries
        ).native_value == pytest.approx(0.30)

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
