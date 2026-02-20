"""Tests for the freshness SLA sensor."""

from orchestrator.sensors.freshness_sensor import (
    _FRESHNESS_SLA_HOURS,
    _MONITORED_ASSETS,
    freshness_sla_sensor,
)


def test_freshness_sensor_defined():
    """freshness_sla_sensor is registered as a sensor."""
    assert freshness_sla_sensor.name == "freshness_sla_sensor"


def test_freshness_sensor_has_minimum_interval():
    """Sensor has a minimum interval configured (runs hourly or less)."""
    assert freshness_sla_sensor.minimum_interval_seconds is not None
    assert freshness_sla_sensor.minimum_interval_seconds <= 3600


def test_monitored_assets_non_empty():
    """At least one asset is monitored for freshness."""
    assert len(_MONITORED_ASSETS) > 0


def test_monitored_assets_include_key_layers():
    """All three lakehouse layers are monitored."""
    assert "bronze_raw_trades" in _MONITORED_ASSETS
    assert "silver_trades" in _MONITORED_ASSETS
    assert "gold_daily_trading_summary" in _MONITORED_ASSETS


def test_freshness_sla_hours_positive():
    """SLA window is a positive number of hours."""
    assert _FRESHNESS_SLA_HOURS > 0
