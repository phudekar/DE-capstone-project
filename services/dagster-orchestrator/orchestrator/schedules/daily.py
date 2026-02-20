"""Daily schedules for Silver micro-batch and Gold aggregation."""

from __future__ import annotations

from dagster import (
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# Silver micro-batch: every 5 minutes
silver_micro_batch_job = define_asset_job(
    name="silver_micro_batch_job",
    selection=AssetSelection.assets(
        AssetKey("silver_trades"),
        AssetKey("silver_orderbook_snapshots"),
        AssetKey("silver_market_data"),
        AssetKey("silver_trader_activity"),
    ),
    description="Process all Silver assets in a micro-batch.",
)

silver_micro_batch_schedule = ScheduleDefinition(
    name="silver_micro_batch_schedule",
    job=silver_micro_batch_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run Silver processing every 5 minutes.",
)

# Daily Gold aggregation: weekdays at 5 PM UTC
gold_daily_job = define_asset_job(
    name="gold_daily_job",
    selection=AssetSelection.assets(
        AssetKey("gold_daily_trading_summary"),
        AssetKey("gold_trader_performance"),
        AssetKey("gold_market_overview"),
        AssetKey("gold_portfolio_positions"),
    ),
    description="Aggregate all Gold assets for the day.",
)

daily_gold_schedule = ScheduleDefinition(
    name="daily_gold_schedule",
    job=gold_daily_job,
    cron_schedule="0 17 * * 1-5",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run Gold aggregation at 5 PM UTC on weekdays.",
)
