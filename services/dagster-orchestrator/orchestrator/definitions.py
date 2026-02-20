"""Dagster Definitions — wires together all assets, resources, sensors, schedules, and jobs."""

from dagster import Definitions

from orchestrator.assets import (
    bronze_raw_marketdata,
    bronze_raw_orderbook,
    bronze_raw_trades,
    dim_account,
    dim_exchange,
    dim_symbol,
    dim_time,
    dim_trader,
    gold_daily_trading_summary,
    gold_market_overview,
    gold_portfolio_positions,
    gold_trader_performance,
    silver_market_data,
    silver_orderbook_snapshots,
    silver_trades,
    silver_trader_activity,
)
from orchestrator.hooks.alerting import slack_alert_on_failure
from orchestrator.jobs.data_quality import data_quality_job
from orchestrator.jobs.export import export_job
from orchestrator.jobs.maintenance import maintenance_job
from orchestrator.resources.duckdb_resource import DuckDBResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.prometheus import PrometheusResource
from orchestrator.schedules.daily import daily_gold_schedule, silver_micro_batch_schedule
from orchestrator.schedules.monthly import monthly_recompute_schedule
from orchestrator.schedules.weekly import weekly_dimension_schedule, weekly_maintenance_schedule
from orchestrator.sensors.kafka_sensor import kafka_orderbook_sensor, kafka_trades_sensor
from orchestrator.sensors.run_status_sensor import (
    bronze_success_trigger_silver,
    run_failure_sensor,
)

defs = Definitions(
    assets=[
        # Bronze
        bronze_raw_trades,
        bronze_raw_orderbook,
        bronze_raw_marketdata,
        # Silver
        silver_trades,
        silver_orderbook_snapshots,
        silver_market_data,
        silver_trader_activity,
        # Gold
        gold_daily_trading_summary,
        gold_trader_performance,
        gold_market_overview,
        gold_portfolio_positions,
        # Dimensions
        dim_symbol,
        dim_time,
        dim_trader,
        dim_exchange,
        dim_account,
    ],
    resources={
        "kafka": KafkaResource(),
        "iceberg": IcebergResource(),
        "duckdb_resource": DuckDBResource(),
        "prometheus": PrometheusResource(),
    },
    sensors=[
        kafka_trades_sensor,
        kafka_orderbook_sensor,
        bronze_success_trigger_silver,
        run_failure_sensor,
    ],
    schedules=[
        silver_micro_batch_schedule,
        daily_gold_schedule,
        weekly_dimension_schedule,
        weekly_maintenance_schedule,
        monthly_recompute_schedule,
    ],
    jobs=[
        maintenance_job,
        data_quality_job,
        export_job,
    ],
)
