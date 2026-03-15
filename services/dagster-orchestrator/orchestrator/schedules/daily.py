"""Daily schedules for Silver micro-batch and Gold aggregation."""

from __future__ import annotations

from datetime import date

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from orchestrator.partitions.daily import daily_partitions

# Silver micro-batch: every 5 minutes
silver_micro_batch_job = define_asset_job(
    name="silver_micro_batch_job",
    selection=AssetSelection.assets(
        AssetKey("silver_trades"),
        AssetKey("silver_orderbook_snapshots"),
        AssetKey("silver_market_data"),
        AssetKey("silver_trader_activity"),
    ),
    partitions_def=daily_partitions,
    description="Process all Silver assets in a micro-batch.",
)


@schedule(
    job=silver_micro_batch_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run Silver processing every 5 minutes for today's partition.",
)
def silver_micro_batch_schedule(context: ScheduleEvaluationContext):
    partition_key = date.today().isoformat()
    yield RunRequest(partition_key=partition_key, run_key=f"silver-{partition_key}-{context.scheduled_execution_time}")


# Daily Gold aggregation: weekdays at 5 PM UTC
gold_daily_job = define_asset_job(
    name="gold_daily_job",
    selection=AssetSelection.assets(
        AssetKey("gold_daily_trading_summary"),
        AssetKey("gold_trader_performance"),
        AssetKey("gold_market_overview"),
        AssetKey("gold_portfolio_positions"),
    ),
    partitions_def=daily_partitions,
    description="Aggregate all Gold assets for the day.",
)


@schedule(
    job=gold_daily_job,
    cron_schedule="0 17 * * 1-5",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run Gold aggregation at 5 PM UTC on weekdays for today's partition.",
)
def daily_gold_schedule(context: ScheduleEvaluationContext):
    partition_key = date.today().isoformat()
    yield RunRequest(partition_key=partition_key, run_key=f"gold-{partition_key}")
