"""Freshness SLA sensor — alerts when assets have not been materialized within SLA."""

import logging
from datetime import datetime, timedelta, timezone

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

logger = logging.getLogger(__name__)

# SLA: each asset must be materialized at least once within this window
_FRESHNESS_SLA_HOURS = 24

# Assets to monitor (asset key strings)
_MONITORED_ASSETS = [
    "bronze_raw_trades",
    "silver_trades",
    "gold_daily_trading_summary",
]


@sensor(
    name="freshness_sla_sensor",
    description=(
        "Monitors asset materialization lag. Triggers data_quality_job when any "
        "monitored asset exceeds the freshness SLA."
    ),
    minimum_interval_seconds=3600,  # Check hourly
)
def freshness_sla_sensor(context: SensorEvaluationContext):
    """Yield a RunRequest for the data_quality_job when SLA is breached."""
    now = datetime.now(tz=timezone.utc)
    sla_cutoff = now - timedelta(hours=_FRESHNESS_SLA_HOURS)

    stale_assets = []
    for asset_key in _MONITORED_ASSETS:
        try:
            record = context.instance.get_latest_materialization_event(
                asset_key=asset_key
            )
            if record is None:
                stale_assets.append(f"{asset_key} (never materialized)")
            else:
                materialized_at = datetime.fromtimestamp(
                    record.timestamp, tz=timezone.utc
                )
                if materialized_at < sla_cutoff:
                    lag_hours = (now - materialized_at).total_seconds() / 3600
                    stale_assets.append(f"{asset_key} (last: {lag_hours:.1f}h ago)")
        except Exception as exc:
            logger.warning("Could not check freshness for %s: %s", asset_key, exc)

    if stale_assets:
        context.log.warning(
            "Freshness SLA breach detected for: %s. Triggering data_quality_job.",
            stale_assets,
        )
        yield RunRequest(
            run_key=f"freshness_sla_{now.strftime('%Y%m%d_%H')}",
            run_config={},
            tags={"trigger": "freshness_sla_sensor", "stale_assets": str(stale_assets)},
        )
    else:
        yield SkipReason("All monitored assets are within freshness SLA.")
