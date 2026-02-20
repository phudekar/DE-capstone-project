"""Run status sensors — trigger downstream pipelines on success/failure."""

import logging
import os

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    RunStatusSensorContext,
    run_failure_sensor as dagster_run_failure_sensor,
    run_status_sensor,
)

logger = logging.getLogger(__name__)


@run_status_sensor(
    name="bronze_success_trigger_silver",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=None,
    default_status=DefaultSensorStatus.STOPPED,
    description="On successful Bronze materialization, trigger Silver processing.",
)
def bronze_success_trigger_silver(context: RunStatusSensorContext) -> None:
    """When a Bronze asset succeeds, immediately trigger Silver assets."""
    materialized_keys = set()
    for event in context.dagster_run.get_run_event_records():
        if hasattr(event, "asset_key") and event.asset_key:
            materialized_keys.add(event.asset_key.to_user_string())

    bronze_assets = {"bronze_raw_trades", "bronze_raw_orderbook"}
    if materialized_keys & bronze_assets:
        context.log.info(
            "Bronze assets materialized: %s — triggering Silver.",
            materialized_keys & bronze_assets,
        )
        from datetime import date

        partition_key = date.today().isoformat()
        yield RunRequest(
            run_key=f"silver-after-bronze-{context.dagster_run.run_id[:8]}",
            asset_selection=[
                AssetKey("silver_trades"),
                AssetKey("silver_orderbook_snapshots"),
            ],
            partition_key=partition_key,
        )


@dagster_run_failure_sensor(
    name="run_failure_sensor",
    default_status=DefaultSensorStatus.STOPPED,
    description="Log failures. Send to Slack webhook if SLACK_WEBHOOK_URL is configured.",
)
def run_failure_sensor(context: RunStatusSensorContext) -> None:
    """Log run failures and optionally notify via Slack webhook."""
    run = context.dagster_run
    context.log.error(
        "Run %s (%s) FAILED. Job: %s",
        run.run_id[:8],
        run.status.value,
        run.job_name,
    )

    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            import urllib.request
            import json

            payload = json.dumps(
                {"text": f"Dagster run FAILED: {run.job_name} (run {run.run_id[:8]})"}
            ).encode()
            req = urllib.request.Request(
                slack_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            context.log.info("Slack notification sent.")
        except Exception:
            context.log.warning("Failed to send Slack notification.")
    else:
        context.log.info("No SLACK_WEBHOOK_URL configured — skipping notification.")
