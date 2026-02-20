"""Sensor that pushes data observability metrics to Prometheus Pushgateway."""

import logging
from datetime import datetime, timezone

from dagster import SensorEvaluationContext, SkipReason, sensor

from orchestrator.metrics.dagster_metrics import (
    dagster_asset_freshness_seconds,
    push_metrics,
    registry,
)

logger = logging.getLogger(__name__)

# Map from asset key → layer label for Prometheus
_ASSET_LAYER_MAP = {
    "bronze_raw_trades": "bronze",
    "silver_trades": "silver",
    "gold_daily_trading_summary": "gold",
    "gold_ohlcv": "gold",
    "gold_symbol_performance": "gold",
}


@sensor(
    name="prometheus_metrics_sensor",
    description="Pushes asset freshness metrics to Prometheus Pushgateway every minute.",
    minimum_interval_seconds=60,
)
def prometheus_metrics_sensor(context: SensorEvaluationContext):
    """Compute freshness seconds for each monitored asset and push to Pushgateway."""
    now = datetime.now(tz=timezone.utc)
    pushed = 0

    for asset_key, layer in _ASSET_LAYER_MAP.items():
        try:
            record = context.instance.get_latest_materialization_event(
                asset_key=asset_key
            )
            if record is None:
                freshness = float("inf")
            else:
                materialized_at = datetime.fromtimestamp(
                    record.timestamp, tz=timezone.utc
                )
                freshness = (now - materialized_at).total_seconds()

            dagster_asset_freshness_seconds.labels(
                asset_key=asset_key, layer=layer
            ).set(freshness)
            pushed += 1
        except Exception as exc:
            logger.warning("Could not compute freshness for %s: %s", asset_key, exc)

    if pushed > 0:
        push_metrics(job_label="dagster_freshness_sensor")
        context.log.info("Pushed freshness metrics for %d assets", pushed)

    yield SkipReason(f"Metrics pushed for {pushed} assets — no run needed.")
