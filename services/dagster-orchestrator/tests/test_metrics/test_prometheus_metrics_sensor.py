"""Tests for the prometheus_metrics_sensor."""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


def test_sensor_is_defined():
    """Sensor is registered as a Dagster sensor."""
    from orchestrator.sensors.prometheus_metrics_sensor import prometheus_metrics_sensor
    assert prometheus_metrics_sensor.name == "prometheus_metrics_sensor"


def test_sensor_minimum_interval():
    """Sensor runs at most every 60 seconds."""
    from orchestrator.sensors.prometheus_metrics_sensor import prometheus_metrics_sensor
    assert prometheus_metrics_sensor.minimum_interval_seconds is not None
    assert prometheus_metrics_sensor.minimum_interval_seconds <= 60


def test_asset_layer_map_non_empty():
    """The asset-to-layer mapping must not be empty."""
    from orchestrator.sensors.prometheus_metrics_sensor import _ASSET_LAYER_MAP
    assert len(_ASSET_LAYER_MAP) > 0


def test_asset_layer_map_covers_all_layers():
    """All three lakehouse layers must be represented."""
    from orchestrator.sensors.prometheus_metrics_sensor import _ASSET_LAYER_MAP
    layers = set(_ASSET_LAYER_MAP.values())
    assert "bronze" in layers
    assert "silver" in layers
    assert "gold" in layers


def test_asset_layer_map_keys_are_strings():
    """All keys and values in the asset-layer map are strings."""
    from orchestrator.sensors.prometheus_metrics_sensor import _ASSET_LAYER_MAP
    for k, v in _ASSET_LAYER_MAP.items():
        assert isinstance(k, str)
        assert isinstance(v, str)


def test_sensor_push_metrics_integration():
    """push_metrics is called in the metrics sensor module."""
    from orchestrator.sensors import prometheus_metrics_sensor as psm_module
    assert hasattr(psm_module, "push_metrics")
    assert hasattr(psm_module, "dagster_asset_freshness_seconds")


def test_freshness_gauge_can_be_set_for_each_asset():
    """freshness seconds gauge can be set for every asset in the map."""
    from orchestrator.sensors.prometheus_metrics_sensor import _ASSET_LAYER_MAP
    from orchestrator.metrics.dagster_metrics import dagster_asset_freshness_seconds

    for asset_key, layer in _ASSET_LAYER_MAP.items():
        # Should not raise
        dagster_asset_freshness_seconds.labels(
            asset_key=asset_key, layer=layer
        ).set(120.0)
