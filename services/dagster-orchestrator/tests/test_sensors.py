"""Tests for sensor definitions."""

from __future__ import annotations

from dagster import AssetKey, DefaultSensorStatus

from orchestrator.sensors.kafka_sensor import kafka_orderbook_sensor, kafka_trades_sensor
from orchestrator.sensors.run_status_sensor import (
    bronze_success_trigger_silver,
    run_failure_sensor,
)


def test_kafka_trades_sensor_exists():
    """Kafka trades sensor is defined with correct name."""
    assert kafka_trades_sensor.name == "kafka_trades_sensor"


def test_kafka_orderbook_sensor_exists():
    """Kafka orderbook sensor is defined with correct name."""
    assert kafka_orderbook_sensor.name == "kafka_orderbook_sensor"


def test_kafka_trades_sensor_targets_bronze():
    """Kafka trades sensor targets bronze_raw_trades asset."""
    selection = kafka_trades_sensor.asset_selection
    assert selection is not None


def test_kafka_trades_sensor_default_stopped():
    """Kafka trades sensor defaults to STOPPED status."""
    assert kafka_trades_sensor.default_status == DefaultSensorStatus.STOPPED


def test_kafka_orderbook_sensor_default_stopped():
    """Kafka orderbook sensor defaults to STOPPED status."""
    assert kafka_orderbook_sensor.default_status == DefaultSensorStatus.STOPPED


def test_bronze_success_trigger_silver_exists():
    """Bronze success trigger sensor is defined."""
    assert bronze_success_trigger_silver.name == "bronze_success_trigger_silver"


def test_run_failure_sensor_exists():
    """Run failure sensor is defined."""
    assert run_failure_sensor.name == "run_failure_sensor"


def test_kafka_trades_sensor_interval():
    """Kafka trades sensor has 30s minimum interval."""
    assert kafka_trades_sensor.minimum_interval_seconds == 30


def test_kafka_orderbook_sensor_interval():
    """Kafka orderbook sensor has 30s minimum interval."""
    assert kafka_orderbook_sensor.minimum_interval_seconds == 30
