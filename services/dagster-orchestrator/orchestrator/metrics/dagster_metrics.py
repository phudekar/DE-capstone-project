"""
Dagster Prometheus metrics helper.
Pushes metrics to Pushgateway after each run / sensor tick / schedule tick.
"""

import os

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")

registry = CollectorRegistry()

# Run metrics
dagster_run_success_total = Counter(
    "dagster_run_success_total",
    "Total successful Dagster runs",
    ["job_name"],
    registry=registry,
)

dagster_run_failure_total = Counter(
    "dagster_run_failure_total",
    "Total failed Dagster runs",
    ["job_name"],
    registry=registry,
)

dagster_run_duration_seconds = Histogram(
    "dagster_run_duration_seconds",
    "Duration of Dagster runs in seconds",
    ["job_name"],
    buckets=[5, 15, 30, 60, 120, 300, 600, 1800],
    registry=registry,
)

# Asset materialization metrics
dagster_asset_materialization_total = Counter(
    "dagster_asset_materialization_total",
    "Total asset materializations",
    ["asset_key", "layer"],
    registry=registry,
)

dagster_asset_materialization_duration_seconds = Histogram(
    "dagster_asset_materialization_duration_seconds",
    "Duration of asset materialization in seconds",
    ["asset_key", "layer"],
    buckets=[1, 5, 15, 30, 60, 120, 300],
    registry=registry,
)

dagster_asset_freshness_seconds = Gauge(
    "dagster_asset_freshness_seconds",
    "Seconds since last successful materialization",
    ["asset_key", "layer"],
    registry=registry,
)

# Sensor metrics
dagster_sensor_tick_total = Counter(
    "dagster_sensor_tick_total",
    "Total sensor ticks",
    ["sensor_name"],
    registry=registry,
)

dagster_sensor_evaluation_total = Counter(
    "dagster_sensor_evaluation_total",
    "Total sensor evaluations that yielded run requests",
    ["sensor_name"],
    registry=registry,
)

dagster_sensor_skip_total = Counter(
    "dagster_sensor_skip_total",
    "Total sensor ticks that were skipped",
    ["sensor_name"],
    registry=registry,
)

# Schedule metrics
dagster_schedule_tick_total = Counter(
    "dagster_schedule_tick_total",
    "Total schedule ticks",
    ["schedule_name"],
    registry=registry,
)

dagster_schedule_late_total = Counter(
    "dagster_schedule_late_total",
    "Total schedule ticks that started late (> 60s past scheduled time)",
    ["schedule_name"],
    registry=registry,
)


def push_metrics(job_label: str = "dagster") -> None:
    """Push all collected metrics to the Pushgateway."""
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job_label, registry=registry)
    except Exception as exc:
        # Non-fatal: metrics push failure should not affect pipeline
        import logging
        logging.getLogger(__name__).warning("Failed to push metrics: %s", exc)
