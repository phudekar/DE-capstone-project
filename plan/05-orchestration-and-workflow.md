# 05 — Orchestration & Workflow Management

## Overview

This document defines the implementation plan for the **Orchestration & Workflow Management** layer of the Data Engineering Capstone Project. Dagster is the chosen orchestration framework. It will coordinate the full lifecycle of stock-market trade data as it flows from Kafka ingestion through Bronze, Silver, and Gold Iceberg table layers, managing scheduling, retries, backfills, idempotency, sensors, and observability.

---

## Table of Contents

1. [Dagster Project Setup](#1-dagster-project-setup)
2. [Asset-Based Architecture](#2-asset-based-architecture)
3. [Resources](#3-resources)
4. [Scheduling Strategies](#4-scheduling-strategies)
5. [Dependency Management](#5-dependency-management)
6. [Retry Logic & Failure Handling](#6-retry-logic--failure-handling)
7. [Backfilling Historical Data](#7-backfilling-historical-data)
8. [Idempotency](#8-idempotency)
9. [Sensors](#9-sensors)
10. [Dagster Ops & Jobs (Non-Asset Workflows)](#10-dagster-ops--jobs-non-asset-workflows)
11. [Implementation Steps & File Layout](#11-implementation-steps--file-layout)
12. [Testing Strategy](#12-testing-strategy)

---

## 1. Dagster Project Setup

### 1.1 Project Structure

All orchestration code lives under `services/dagster-orchestrator/` in the monorepo.

```
services/dagster-orchestrator/
├── setup.py                        # Python package definition
├── setup.cfg                       # Package metadata / extras
├── pyproject.toml                  # Build system config
├── dagster.yaml                    # Dagster instance configuration
├── workspace.yaml                  # Code location configuration
├── Dockerfile                      # Container image for dagster services
├── docker-compose.dagster.yaml     # Compose file for webserver + daemon + DB
├── orchestrator/
│   ├── __init__.py                 # Definitions entry point
│   ├── definitions.py              # Central Definitions object
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── bronze.py               # Bronze layer assets
│   │   ├── silver.py               # Silver layer assets
│   │   ├── gold.py                 # Gold layer assets
│   │   └── dimensions.py           # Dimension / SCD assets
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── kafka.py                # Kafka consumer/producer resource
│   │   ├── iceberg.py              # PyIceberg catalog resource
│   │   ├── duckdb.py               # DuckDB connection resource
│   │   └── prometheus.py           # Prometheus metrics push resource
│   ├── sensors/
│   │   ├── __init__.py
│   │   ├── kafka_sensor.py         # Polls Kafka for new messages
│   │   ├── freshness_sensor.py     # Monitors asset freshness
│   │   └── run_status_sensor.py    # Reacts to run success/failure
│   ├── schedules/
│   │   ├── __init__.py
│   │   ├── daily.py                # Daily gold aggregation schedule
│   │   ├── weekly.py               # Weekly dimension refresh
│   │   └── monthly.py              # Monthly historical recomputation
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── maintenance.py          # Iceberg compaction / cleanup
│   │   ├── data_quality.py         # Great Expectations trigger
│   │   └── export.py               # Data export for external consumers
│   ├── partitions/
│   │   ├── __init__.py
│   │   └── daily.py                # DailyPartitionsDefinition + helpers
│   ├── hooks/
│   │   ├── __init__.py
│   │   └── alerting.py             # Slack / email failure hooks
│   └── utils/
│       ├── __init__.py
│       ├── kafka_helpers.py         # Offset management utilities
│       └── iceberg_helpers.py       # Iceberg write/merge helpers
└── tests/
    ├── __init__.py
    ├── conftest.py                  # Shared fixtures
    ├── test_bronze_assets.py
    ├── test_silver_assets.py
    ├── test_gold_assets.py
    ├── test_dimension_assets.py
    ├── test_sensors.py
    ├── test_schedules.py
    ├── test_resources.py
    └── test_jobs.py
```

### 1.2 `dagster.yaml` — Instance Configuration

This file configures the Dagster instance services: run storage, event log, schedule storage, and compute log storage. All persistent state is stored in PostgreSQL so that it survives container restarts.

```yaml
# services/dagster-orchestrator/dagster.yaml

# ── Run Storage ──────────────────────────────────────────────
# Stores metadata about each pipeline run (status, timestamps, tags).
run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_db:
      hostname:
        env: DAGSTER_PG_HOST
      port:
        env: DAGSTER_PG_PORT
      username:
        env: DAGSTER_PG_USER
      password:
        env: DAGSTER_PG_PASSWORD
      db_name:
        env: DAGSTER_PG_DB

# ── Event Log Storage ────────────────────────────────────────
# Stores the full event stream (materialization events, log messages,
# asset observations) that powers the Dagster UI timelines.
event_log_storage:
  module: dagster_postgres.event_log
  class: PostgresEventLogStorage
  config:
    postgres_db:
      hostname:
        env: DAGSTER_PG_HOST
      port:
        env: DAGSTER_PG_PORT
      username:
        env: DAGSTER_PG_USER
      password:
        env: DAGSTER_PG_PASSWORD
      db_name:
        env: DAGSTER_PG_DB

# ── Schedule Storage ─────────────────────────────────────────
# Tracks schedule ticks, sensor cursors, and schedule state.
schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_db:
      hostname:
        env: DAGSTER_PG_HOST
      port:
        env: DAGSTER_PG_PORT
      username:
        env: DAGSTER_PG_USER
      password:
        env: DAGSTER_PG_PASSWORD
      db_name:
        env: DAGSTER_PG_DB

# ── Compute Log Manager ─────────────────────────────────────
# Stores stdout/stderr from op/asset executions. Local file storage
# in dev; switch to S3/MinIO-backed storage in prod.
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /opt/dagster/compute_logs

# ── Run Launcher ─────────────────────────────────────────────
# DefaultRunLauncher runs jobs in the same process as the daemon.
# For production, switch to DockerRunLauncher or K8sRunLauncher.
run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

# ── Run Coordinator ──────────────────────────────────────────
# Controls concurrency of runs. QueuedRunCoordinator lets the daemon
# dequeue runs respecting max_concurrent_runs.
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5

# ── Telemetry ────────────────────────────────────────────────
telemetry:
  enabled: false
```

### 1.3 `workspace.yaml` — Code Location

Points Dagster at the Python module that exposes the `Definitions` object.

```yaml
# services/dagster-orchestrator/workspace.yaml

load_from:
  - python_module:
      module_name: orchestrator.definitions
      working_directory: /opt/dagster/app
```

### 1.4 Definitions Entry Point

The `Definitions` object is the single source of truth for all assets, resources, schedules, sensors, and jobs.

```python
# services/dagster-orchestrator/orchestrator/definitions.py

from dagster import Definitions, EnvVar

from orchestrator.assets.bronze import bronze_raw_trades, bronze_raw_orderbook, bronze_raw_marketdata
from orchestrator.assets.silver import silver_trades, silver_orderbook_snapshots, silver_market_data, silver_trader_activity
from orchestrator.assets.gold import gold_daily_trading_summary, gold_trader_performance, gold_market_overview, gold_portfolio_positions
from orchestrator.assets.dimensions import dim_symbol, dim_trader, dim_exchange, dim_time, dim_account

from orchestrator.resources.kafka import KafkaResource
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.duckdb import DuckDBResource
from orchestrator.resources.prometheus import PrometheusResource

from orchestrator.sensors import kafka_sensor, freshness_sensor, run_status_sensor
from orchestrator.schedules import daily_gold_schedule, weekly_dimension_schedule, monthly_recompute_schedule
from orchestrator.jobs import maintenance_job, data_quality_job, export_job

defs = Definitions(
    assets=[
        # Bronze
        bronze_raw_trades, bronze_raw_orderbook, bronze_raw_marketdata,
        # Silver
        silver_trades, silver_orderbook_snapshots, silver_market_data, silver_trader_activity,
        # Gold
        gold_daily_trading_summary, gold_trader_performance, gold_market_overview, gold_portfolio_positions,
        # Dimensions
        dim_symbol, dim_trader, dim_exchange, dim_time, dim_account,
    ],
    resources={
        "kafka": KafkaResource(
            bootstrap_servers=EnvVar("KAFKA_BOOTSTRAP_SERVERS"),
            schema_registry_url=EnvVar("SCHEMA_REGISTRY_URL"),
        ),
        "iceberg": IcebergResource(
            catalog_uri=EnvVar("ICEBERG_CATALOG_URI"),
            warehouse_path=EnvVar("ICEBERG_WAREHOUSE_PATH"),
        ),
        "duckdb": DuckDBResource(
            database_path=EnvVar("DUCKDB_DATABASE_PATH"),
        ),
        "prometheus": PrometheusResource(
            pushgateway_url=EnvVar("PROMETHEUS_PUSHGATEWAY_URL"),
        ),
    },
    schedules=[
        daily_gold_schedule,
        weekly_dimension_schedule,
        monthly_recompute_schedule,
    ],
    sensors=[
        kafka_sensor,
        freshness_sensor,
        run_status_sensor,
    ],
    jobs=[
        maintenance_job,
        data_quality_job,
        export_job,
    ],
)
```

### 1.5 Dagster Services — Docker Compose

Three containers run the Dagster services: the **webserver** (Dagit UI), the **daemon** (schedules, sensors, run queue), and a **PostgreSQL** backend.

```yaml
# services/dagster-orchestrator/docker-compose.dagster.yaml

version: "3.8"

services:
  dagster-postgres:
    image: postgres:16-alpine
    container_name: dagster-postgres
    environment:
      POSTGRES_USER: dagster
      POSTGRES_PASSWORD: dagster_secret
      POSTGRES_DB: dagster
    ports:
      - "5433:5432"
    volumes:
      - dagster_pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dagster"]
      interval: 5s
      timeout: 3s
      retries: 5

  dagster-webserver:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dagster-webserver
    entrypoint:
      - dagster-webserver
      - -h
      - "0.0.0.0"
      - -p
      - "3000"
      - -w
      - workspace.yaml
    ports:
      - "3000:3000"
    env_file:
      - .env
    environment:
      DAGSTER_PG_HOST: dagster-postgres
      DAGSTER_PG_PORT: "5432"
      DAGSTER_PG_USER: dagster
      DAGSTER_PG_PASSWORD: dagster_secret
      DAGSTER_PG_DB: dagster
    volumes:
      - /opt/dagster/compute_logs
    depends_on:
      dagster-postgres:
        condition: service_healthy

  dagster-daemon:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: dagster-daemon
    entrypoint:
      - dagster-daemon
      - run
    env_file:
      - .env
    environment:
      DAGSTER_PG_HOST: dagster-postgres
      DAGSTER_PG_PORT: "5432"
      DAGSTER_PG_USER: dagster
      DAGSTER_PG_PASSWORD: dagster_secret
      DAGSTER_PG_DB: dagster
    volumes:
      - /opt/dagster/compute_logs
    depends_on:
      dagster-postgres:
        condition: service_healthy

volumes:
  dagster_pg_data:
```

### 1.6 Dockerfile

```dockerfile
# services/dagster-orchestrator/Dockerfile

FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    dagster-docker

WORKDIR /opt/dagster/app

COPY setup.py setup.cfg pyproject.toml ./
COPY orchestrator/ orchestrator/
COPY dagster.yaml workspace.yaml /opt/dagster/

RUN pip install -e ".[dev]"

ENV DAGSTER_HOME=/opt/dagster
```

### 1.7 `setup.py`

```python
# services/dagster-orchestrator/setup.py

from setuptools import find_packages, setup

setup(
    name="orchestrator",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster>=1.7,<2.0",
        "dagster-webserver",
        "dagster-postgres",
        "pyiceberg>=0.6.0",
        "duckdb>=0.10",
        "confluent-kafka>=2.3",
        "prometheus-client>=0.20",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
            "pytest",
            "pytest-cov",
        ],
    },
)
```

---

## 2. Asset-Based Architecture

Dagster Software-Defined Assets (SDAs) are the primary abstraction. Every table in the Bronze, Silver, and Gold layers is modeled as an asset with explicit upstream dependencies, metadata, and freshness policies.

### 2.1 Bronze Assets — Ingestion Layer

Bronze assets represent raw data landing from Kafka topics into Iceberg tables with no transformation beyond serialization.

**File**: `orchestrator/assets/bronze.py`

```python
import json
from dagster import (
    asset,
    AssetKey,
    AutoMaterializePolicy,
    FreshnessPolicy,
    MetadataValue,
    Output,
    RetryPolicy,
)

BRONZE_GROUP = "bronze"

BRONZE_RETRY_POLICY = RetryPolicy(
    max_retries=5,
    delay=30,  # seconds
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


@asset(
    group_name=BRONZE_GROUP,
    key_prefix=["bronze"],
    compute_kind="kafka-to-iceberg",
    description="Ingest raw trade events from Kafka topic 'raw.trades' into "
                "the Bronze Iceberg table bronze.raw_trades. Each run consumes "
                "all available messages since the last committed offset.",
    retry_policy=BRONZE_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    metadata={
        "kafka_topic": "raw.trades",
        "iceberg_table": "bronze.raw_trades",
        "owner": "data-engineering",
    },
)
def bronze_raw_trades(context, kafka: KafkaResource, iceberg: IcebergResource):
    """
    Consume messages from the Kafka 'raw.trades' topic and append them as-is
    to the bronze.raw_trades Iceberg table.

    Steps:
      1. Create a Kafka consumer subscribed to 'raw.trades'.
      2. Poll messages up to a configurable batch size / timeout.
      3. Deserialize each message (Avro via Schema Registry).
      4. Write the batch to the Iceberg table as an append operation.
      5. Commit Kafka offsets only after successful Iceberg write.
      6. Emit materialization metadata (row count, byte size, offsets).
    """
    consumer = kafka.get_consumer(group_id="dagster-bronze-trades")
    table = iceberg.load_table("bronze.raw_trades")

    records = kafka.consume_batch(
        consumer=consumer,
        topic="raw.trades",
        max_messages=10_000,
        timeout_seconds=30,
    )

    if not records:
        context.log.info("No new messages on raw.trades — skipping.")
        return Output(
            value=None,
            metadata={"row_count": MetadataValue.int(0)},
        )

    row_count = iceberg.append_records(table, records)
    kafka.commit_offsets(consumer)

    context.log.info(f"Wrote {row_count} records to bronze.raw_trades")
    return Output(
        value=None,
        metadata={
            "row_count": MetadataValue.int(row_count),
            "kafka_topic": MetadataValue.text("raw.trades"),
        },
    )


@asset(
    group_name=BRONZE_GROUP,
    key_prefix=["bronze"],
    compute_kind="kafka-to-iceberg",
    description="Ingest raw orderbook snapshots from Kafka topic 'raw.orderbook' "
                "into the Bronze Iceberg table bronze.raw_orderbook.",
    retry_policy=BRONZE_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    metadata={
        "kafka_topic": "raw.orderbook",
        "iceberg_table": "bronze.raw_orderbook",
    },
)
def bronze_raw_orderbook(context, kafka: KafkaResource, iceberg: IcebergResource):
    """
    Same pattern as bronze_raw_trades but for the orderbook topic.
    """
    # Implementation follows the same consume-append-commit pattern.
    ...


@asset(
    group_name=BRONZE_GROUP,
    key_prefix=["bronze"],
    compute_kind="kafka-to-iceberg",
    description="Ingest raw market data (OHLCV) from Kafka topic 'raw.marketdata' "
                "into the Bronze Iceberg table bronze.raw_marketdata.",
    retry_policy=BRONZE_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    metadata={
        "kafka_topic": "raw.marketdata",
        "iceberg_table": "bronze.raw_marketdata",
    },
)
def bronze_raw_marketdata(context, kafka: KafkaResource, iceberg: IcebergResource):
    """
    Same pattern as bronze_raw_trades but for the marketdata topic.
    """
    ...
```

**Key design decisions for Bronze:**

| Concern | Decision |
|---|---|
| Trigger | Sensor-based — Kafka topic sensor detects new messages and triggers materialization |
| Write mode | Append-only — raw data is immutable at the Bronze layer |
| Offset management | Kafka consumer offsets are committed **after** successful Iceberg write to achieve at-least-once delivery |
| Schema | Stored exactly as received (Avro-deserialized to dict/struct); no casting or renaming |
| Partitioning | Iceberg table partitioned by `ingestion_date` (day granularity) for efficient downstream reads |

---

### 2.2 Silver Assets — Transformation Layer

Silver assets apply data cleaning, deduplication, type casting, and null handling. Each Silver asset declares explicit dependencies on its upstream Bronze asset.

**File**: `orchestrator/assets/silver.py`

```python
from dagster import (
    asset,
    AssetIn,
    FreshnessPolicy,
    MetadataValue,
    Output,
    RetryPolicy,
    Backoff,
    Jitter,
)

SILVER_GROUP = "silver"

SILVER_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)


@asset(
    group_name=SILVER_GROUP,
    key_prefix=["silver"],
    ins={"bronze_raw_trades": AssetIn(key=AssetKey(["bronze", "bronze_raw_trades"]))},
    compute_kind="duckdb-transform",
    description=(
        "Clean and deduplicate trade records from Bronze. "
        "Dedup by trade_id, cast columns to strict types, handle nulls, "
        "and write to silver.trades via idempotent merge."
    ),
    retry_policy=SILVER_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=15),
    metadata={
        "iceberg_table": "silver.trades",
        "primary_key": "trade_id",
        "dedup_strategy": "latest_by_event_timestamp",
    },
)
def silver_trades(
    context,
    bronze_raw_trades,
    iceberg: IcebergResource,
    duckdb: DuckDBResource,
):
    """
    Transform bronze.raw_trades -> silver.trades.

    Processing steps:
      1. Read new records from bronze.raw_trades that have not yet been
         processed (tracked via high-watermark on ingestion_timestamp).
      2. Load into DuckDB in-memory for transformation.
      3. Deduplicate by trade_id, keeping the record with the latest
         event_timestamp.
      4. Cast columns:
           - price: DOUBLE
           - quantity: DOUBLE
           - trade_timestamp: TIMESTAMP WITH TIME ZONE
           - symbol: VARCHAR (upper-cased, trimmed)
      5. Handle nulls:
           - Drop rows where trade_id is NULL.
           - Default quantity to 0 where NULL.
           - Default price to 0.0 where NULL (flagged for review).
      6. Write to silver.trades Iceberg table using MERGE INTO on trade_id
         (upsert semantics for idempotency).
      7. Emit metadata: row count, dedup count, null counts.
    """
    conn = duckdb.get_connection()

    # Step 1: Read unprocessed bronze records
    bronze_df = iceberg.read_new_records(
        table_name="bronze.raw_trades",
        watermark_column="ingestion_timestamp",
        context=context,
    )

    if bronze_df.is_empty():
        context.log.info("No new bronze records to process.")
        return Output(value=None, metadata={"row_count": MetadataValue.int(0)})

    # Step 2-5: Transform in DuckDB
    conn.register("bronze_trades", bronze_df)
    result = conn.execute("""
        WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY trade_id
                       ORDER BY event_timestamp DESC
                   ) AS rn
            FROM bronze_trades
            WHERE trade_id IS NOT NULL
        )
        SELECT
            trade_id,
            UPPER(TRIM(symbol))         AS symbol,
            CAST(price AS DOUBLE)       AS price,
            COALESCE(CAST(quantity AS DOUBLE), 0.0) AS quantity,
            side,
            trader_id,
            exchange,
            CAST(trade_timestamp AS TIMESTAMPTZ) AS trade_timestamp,
            event_timestamp
        FROM deduped
        WHERE rn = 1
    """).fetchdf()

    # Step 6: Merge into Iceberg
    rows_written = iceberg.merge_records(
        table_name="silver.trades",
        dataframe=result,
        merge_key="trade_id",
    )

    context.log.info(f"Merged {rows_written} records into silver.trades")
    return Output(
        value=None,
        metadata={
            "row_count": MetadataValue.int(rows_written),
            "bronze_input_rows": MetadataValue.int(len(bronze_df)),
            "dedup_removed": MetadataValue.int(len(bronze_df) - rows_written),
        },
    )


@asset(
    group_name=SILVER_GROUP,
    key_prefix=["silver"],
    ins={"bronze_raw_orderbook": AssetIn(key=AssetKey(["bronze", "bronze_raw_orderbook"]))},
    compute_kind="duckdb-transform",
    description="Clean orderbook snapshots: validate bid/ask spread, "
                "remove stale snapshots, normalize price levels.",
    retry_policy=SILVER_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=15),
    metadata={"iceberg_table": "silver.orderbook_snapshots"},
)
def silver_orderbook_snapshots(context, bronze_raw_orderbook, iceberg, duckdb):
    """
    Transform bronze.raw_orderbook -> silver.orderbook_snapshots.
    - Remove snapshots where best_bid >= best_ask (crossed book).
    - Normalize price levels to 2 decimal places.
    - Deduplicate by (symbol, snapshot_timestamp).
    """
    ...


@asset(
    group_name=SILVER_GROUP,
    key_prefix=["silver"],
    ins={"bronze_raw_marketdata": AssetIn(key=AssetKey(["bronze", "bronze_raw_marketdata"]))},
    compute_kind="duckdb-transform",
    description="Clean OHLCV market data: validate OHLC relationships, "
                "handle missing volume, cast to correct types.",
    retry_policy=SILVER_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=15),
    metadata={"iceberg_table": "silver.market_data"},
)
def silver_market_data(context, bronze_raw_marketdata, iceberg, duckdb):
    """
    Transform bronze.raw_marketdata -> silver.market_data.
    - Validate: high >= low, open and close within [low, high].
    - Default volume to 0 where NULL.
    - Deduplicate by (symbol, interval_start).
    """
    ...


@asset(
    group_name=SILVER_GROUP,
    key_prefix=["silver"],
    ins={"silver_trades_dep": AssetIn(key=AssetKey(["silver", "silver_trades"]))},
    compute_kind="duckdb-transform",
    description="Aggregate trader-level activity metrics from silver.trades: "
                "trade counts, total volume, distinct symbols, per micro-batch window.",
    retry_policy=SILVER_RETRY_POLICY,
    metadata={"iceberg_table": "silver.trader_activity"},
)
def silver_trader_activity(context, silver_trades_dep, iceberg, duckdb):
    """
    Aggregate silver.trades -> silver.trader_activity.
    - GROUP BY trader_id, DATE_TRUNC('hour', trade_timestamp).
    - Compute: trade_count, total_volume, total_notional, distinct_symbols.
    - Merge by (trader_id, activity_hour).
    """
    ...
```

**Silver layer transformation rules summary:**

| Asset | Source | Primary Key | Dedup Strategy | Null Handling |
|---|---|---|---|---|
| `silver_trades` | `bronze_raw_trades` | `trade_id` | Latest by `event_timestamp` | Drop if `trade_id` NULL; default quantity=0, price=0.0 |
| `silver_orderbook_snapshots` | `bronze_raw_orderbook` | `(symbol, snapshot_timestamp)` | Latest by `event_timestamp` | Drop if symbol NULL |
| `silver_market_data` | `bronze_raw_marketdata` | `(symbol, interval_start)` | Latest by `event_timestamp` | Default volume=0 |
| `silver_trader_activity` | `silver_trades` | `(trader_id, activity_hour)` | Re-aggregate on each run | N/A — derived |

---

### 2.3 Gold Assets — Aggregation Layer

Gold assets produce business-ready aggregations for analytics and reporting. They are scheduled daily after market close (5:00 PM ET).

**File**: `orchestrator/assets/gold.py`

```python
from dagster import (
    asset,
    AssetIn,
    AssetKey,
    DailyPartitionsDefinition,
    FreshnessPolicy,
    MetadataValue,
    Output,
    RetryPolicy,
    Backoff,
    Jitter,
)

GOLD_GROUP = "gold"

GOLD_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=120,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# Partitioned by trading day — allows targeted backfills.
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="America/New_York",
)


@asset(
    group_name=GOLD_GROUP,
    key_prefix=["gold"],
    ins={
        "silver_trades": AssetIn(key=AssetKey(["silver", "silver_trades"])),
        "silver_market_data": AssetIn(key=AssetKey(["silver", "silver_market_data"])),
    },
    partitions_def=daily_partitions,
    compute_kind="duckdb-aggregate",
    description=(
        "Daily trading summary per symbol: open/close/high/low prices, "
        "total volume, trade count, VWAP, and spread metrics. "
        "Partition = one trading day."
    ),
    retry_policy=GOLD_RETRY_POLICY,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=120),
    metadata={
        "iceberg_table": "gold.daily_trading_summary",
        "partition_column": "trading_date",
    },
)
def gold_daily_trading_summary(context, silver_trades, silver_market_data, iceberg, duckdb):
    """
    Aggregate silver.trades + silver.market_data -> gold.daily_trading_summary.

    For each (symbol, trading_date) partition:
      - open_price: first trade price of the day
      - close_price: last trade price of the day
      - high_price: max trade price
      - low_price: min trade price
      - total_volume: sum of quantity
      - trade_count: count of trades
      - vwap: volume-weighted average price = SUM(price * quantity) / SUM(quantity)
      - avg_spread: average (best_ask - best_bid) from orderbook snapshots

    Write mode: overwrite the partition (full recompute for idempotency).
    """
    partition_date = context.partition_key  # e.g. "2024-06-15"
    conn = duckdb.get_connection()

    trades_df = iceberg.read_partition(
        table_name="silver.trades",
        partition_column="trade_date",
        partition_value=partition_date,
    )

    market_df = iceberg.read_partition(
        table_name="silver.market_data",
        partition_column="market_date",
        partition_value=partition_date,
    )

    conn.register("trades", trades_df)
    conn.register("market", market_df)

    result = conn.execute("""
        SELECT
            t.symbol,
            CAST(:partition_date AS DATE)                    AS trading_date,
            FIRST(t.price ORDER BY t.trade_timestamp)        AS open_price,
            LAST(t.price ORDER BY t.trade_timestamp)         AS close_price,
            MAX(t.price)                                     AS high_price,
            MIN(t.price)                                     AS low_price,
            SUM(t.quantity)                                  AS total_volume,
            COUNT(*)                                         AS trade_count,
            SUM(t.price * t.quantity) / NULLIF(SUM(t.quantity), 0) AS vwap,
            AVG(m.close - m.open)                            AS avg_price_change
        FROM trades t
        LEFT JOIN market m ON t.symbol = m.symbol
        GROUP BY t.symbol
    """, {"partition_date": partition_date}).fetchdf()

    rows = iceberg.overwrite_partition(
        table_name="gold.daily_trading_summary",
        dataframe=result,
        partition_column="trading_date",
        partition_value=partition_date,
    )

    return Output(
        value=None,
        metadata={
            "row_count": MetadataValue.int(rows),
            "partition": MetadataValue.text(partition_date),
            "symbols_covered": MetadataValue.int(result["symbol"].nunique()),
        },
    )


@asset(
    group_name=GOLD_GROUP,
    key_prefix=["gold"],
    ins={
        "silver_trades": AssetIn(key=AssetKey(["silver", "silver_trades"])),
        "silver_trader_activity": AssetIn(key=AssetKey(["silver", "silver_trader_activity"])),
    },
    partitions_def=daily_partitions,
    compute_kind="duckdb-aggregate",
    description="Daily trader performance metrics: PnL, win rate, Sharpe ratio proxy, "
                "average hold time, max drawdown.",
    retry_policy=GOLD_RETRY_POLICY,
    metadata={"iceberg_table": "gold.trader_performance"},
)
def gold_trader_performance(context, silver_trades, silver_trader_activity, iceberg, duckdb):
    """
    Aggregate per trader per day:
      - realized_pnl: sum of (sell_price - avg_buy_price) * quantity for closed positions
      - win_rate: fraction of profitable trades
      - trade_count, total_volume
      - max_drawdown: largest peak-to-trough decline in cumulative PnL
    """
    ...


@asset(
    group_name=GOLD_GROUP,
    key_prefix=["gold"],
    ins={
        "gold_daily_trading_summary": AssetIn(key=AssetKey(["gold", "gold_daily_trading_summary"])),
    },
    partitions_def=daily_partitions,
    compute_kind="duckdb-aggregate",
    description="Market-wide overview: total volume across all symbols, "
                "market breadth (advancers vs. decliners), top movers.",
    retry_policy=GOLD_RETRY_POLICY,
    metadata={"iceberg_table": "gold.market_overview"},
)
def gold_market_overview(context, gold_daily_trading_summary, iceberg, duckdb):
    """
    Aggregate gold.daily_trading_summary -> gold.market_overview.
      - total_market_volume
      - advancing_symbols (close > open)
      - declining_symbols (close < open)
      - top_5_gainers, top_5_losers (by % change)
      - market_vwap (volume-weighted across all symbols)
    """
    ...


@asset(
    group_name=GOLD_GROUP,
    key_prefix=["gold"],
    ins={
        "silver_trades": AssetIn(key=AssetKey(["silver", "silver_trades"])),
    },
    partitions_def=daily_partitions,
    compute_kind="duckdb-aggregate",
    description="Current portfolio positions per trader: open position quantity, "
                "average entry price, unrealized PnL, position value.",
    retry_policy=GOLD_RETRY_POLICY,
    metadata={"iceberg_table": "gold.portfolio_positions"},
)
def gold_portfolio_positions(context, silver_trades, iceberg, duckdb):
    """
    Compute net position per (trader_id, symbol) as of partition date.
      - net_quantity = SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END)
      - avg_entry_price = weighted average of buy prices
      - current_price = last trade price of the day
      - unrealized_pnl = (current_price - avg_entry_price) * net_quantity
    Only include positions where net_quantity != 0.
    """
    ...
```

**Gold layer asset summary:**

| Asset | Dependencies | Partition | Schedule | Write Mode |
|---|---|---|---|---|
| `gold_daily_trading_summary` | silver_trades, silver_market_data | Daily | 5:00 PM ET | Overwrite partition |
| `gold_trader_performance` | silver_trades, silver_trader_activity | Daily | 5:00 PM ET | Overwrite partition |
| `gold_market_overview` | gold_daily_trading_summary | Daily | 5:00 PM ET (after summary) | Overwrite partition |
| `gold_portfolio_positions` | silver_trades | Daily | 5:00 PM ET | Overwrite partition |

---

### 2.4 Dimension Assets

Dimension assets manage slowly-changing dimension (SCD) tables that enrichment assets in the Silver and Gold layers reference.

**File**: `orchestrator/assets/dimensions.py`

```python
from dagster import asset, RetryPolicy

DIM_GROUP = "dimensions"

DIM_RETRY_POLICY = RetryPolicy(max_retries=2, delay=60)


@asset(
    group_name=DIM_GROUP,
    key_prefix=["dim"],
    compute_kind="scd-processing",
    description="Symbol dimension: ticker symbol, company name, sector, exchange, "
                "listing date, market cap bucket. SCD Type 2 — tracks symbol renames "
                "and sector reclassifications.",
    retry_policy=DIM_RETRY_POLICY,
    metadata={
        "iceberg_table": "dim.symbol",
        "scd_type": "2",
        "business_key": "symbol",
        "tracked_columns": "company_name, sector, market_cap_bucket",
    },
)
def dim_symbol(context, iceberg, duckdb):
    """
    SCD Type 2 processing for dim.symbol.

    Steps:
      1. Read the current dim.symbol table (where is_current = true).
      2. Read the latest symbol reference data (from a seed file or API).
      3. Compare on business_key = symbol.
      4. For changed records:
         a. Set is_current = false, valid_to = now() on the old row.
         b. Insert new row with is_current = true, valid_from = now(), valid_to = NULL.
      5. For new records: insert with is_current = true.
      6. For unchanged records: no action.
    """
    ...


@asset(
    group_name=DIM_GROUP,
    key_prefix=["dim"],
    compute_kind="scd-processing",
    description="Trader dimension: trader ID, name, firm, desk, risk tier. "
                "SCD Type 2 — tracks firm/desk changes.",
    retry_policy=DIM_RETRY_POLICY,
    metadata={
        "iceberg_table": "dim.trader",
        "scd_type": "2",
        "business_key": "trader_id",
        "tracked_columns": "firm, desk, risk_tier",
    },
)
def dim_trader(context, iceberg, duckdb):
    """SCD Type 2 for trader dimension."""
    ...


@asset(
    group_name=DIM_GROUP,
    key_prefix=["dim"],
    compute_kind="scd-processing",
    description="Exchange dimension: exchange code, name, timezone, operating hours. "
                "SCD Type 1 — overwrite on change.",
    retry_policy=DIM_RETRY_POLICY,
    metadata={
        "iceberg_table": "dim.exchange",
        "scd_type": "1",
        "business_key": "exchange_code",
    },
)
def dim_exchange(context, iceberg, duckdb):
    """SCD Type 1 for exchange dimension — overwrite changed records."""
    ...


@asset(
    group_name=DIM_GROUP,
    key_prefix=["dim"],
    compute_kind="generated",
    description="Time dimension: pre-generated calendar table covering 2020-2030 "
                "with trading day flags, fiscal periods, and market session indicators.",
    retry_policy=DIM_RETRY_POLICY,
    metadata={
        "iceberg_table": "dim.time",
        "scd_type": "N/A — static reference",
    },
)
def dim_time(context, iceberg, duckdb):
    """
    Generate or refresh the time dimension.
    - date_key (INT, YYYYMMDD), full_date, year, quarter, month, day,
      day_of_week, is_weekend, is_trading_day, fiscal_year, fiscal_quarter,
      market_session (pre-market, regular, after-hours, closed).
    """
    ...


@asset(
    group_name=DIM_GROUP,
    key_prefix=["dim"],
    compute_kind="scd-processing",
    description="Account dimension: account ID, account type (margin, cash), "
                "status, creation date. SCD Type 3 — track previous account type.",
    retry_policy=DIM_RETRY_POLICY,
    metadata={
        "iceberg_table": "dim.account",
        "scd_type": "3",
        "business_key": "account_id",
        "tracked_columns": "account_type, status",
    },
)
def dim_account(context, iceberg, duckdb):
    """
    SCD Type 3 for account dimension.
    - Maintains current_account_type and previous_account_type columns.
    - On change: shift current -> previous, write new value to current.
    """
    ...
```

**Dimension SCD strategy:**

| Dimension | SCD Type | Business Key | Tracked Columns | Refresh Cadence |
|---|---|---|---|---|
| `dim_symbol` | 2 | `symbol` | company_name, sector, market_cap_bucket | Weekly |
| `dim_trader` | 2 | `trader_id` | firm, desk, risk_tier | Weekly |
| `dim_exchange` | 1 | `exchange_code` | name, timezone, operating_hours | Weekly |
| `dim_time` | Static | `date_key` | N/A | On demand |
| `dim_account` | 3 | `account_id` | account_type, status | Weekly |

---

## 3. Resources

Dagster resources encapsulate connections to external systems. They are configured once in the `Definitions` object and injected into every asset that requests them.

### 3.1 Kafka Resource

**File**: `orchestrator/resources/kafka.py`

```python
from dagster import ConfigurableResource
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pydantic import Field
from typing import Optional


class KafkaResource(ConfigurableResource):
    """
    Wraps confluent-kafka Consumer and Producer with Schema Registry
    integration. Provides helper methods for batch consumption and
    offset management.
    """

    bootstrap_servers: str = Field(
        description="Comma-separated list of Kafka broker addresses."
    )
    schema_registry_url: str = Field(
        description="URL of the Confluent Schema Registry."
    )
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol (PLAINTEXT, SSL, SASL_SSL)."
    )
    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)."
    )
    sasl_username: Optional[str] = Field(default=None)
    sasl_password: Optional[str] = Field(default=None)

    def get_consumer(self, group_id: str, **overrides) -> Consumer:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Manual commit after write
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.sasl_password
        config.update(overrides)
        return Consumer(config)

    def get_producer(self, **overrides) -> Producer:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        config.update(overrides)
        return Producer(config)

    def get_deserializer(self, subject: str) -> AvroDeserializer:
        sr_client = SchemaRegistryClient({"url": self.schema_registry_url})
        return AvroDeserializer(sr_client)

    def consume_batch(self, consumer, topic, max_messages=10_000, timeout_seconds=30):
        """
        Poll messages from a topic up to max_messages or timeout.
        Returns a list of deserialized records.
        """
        consumer.subscribe([topic])
        records = []
        deadline = time.time() + timeout_seconds

        while len(records) < max_messages and time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            records.append(msg.value())

        return records

    def commit_offsets(self, consumer):
        """Synchronously commit current offsets."""
        consumer.commit(asynchronous=False)
```

### 3.2 Iceberg Resource

**File**: `orchestrator/resources/iceberg.py`

```python
from dagster import ConfigurableResource
from pyiceberg.catalog import load_catalog
from pydantic import Field


class IcebergResource(ConfigurableResource):
    """
    Wraps PyIceberg catalog operations. Provides helpers for reading,
    appending, merging, and partition-level overwriting of Iceberg tables.
    """

    catalog_uri: str = Field(description="URI for the Iceberg REST catalog.")
    warehouse_path: str = Field(description="Warehouse root path (e.g., s3://bucket/warehouse).")
    catalog_name: str = Field(default="default", description="Catalog name.")

    def _get_catalog(self):
        return load_catalog(
            self.catalog_name,
            **{
                "uri": self.catalog_uri,
                "warehouse": self.warehouse_path,
            },
        )

    def load_table(self, table_name: str):
        catalog = self._get_catalog()
        return catalog.load_table(table_name)

    def append_records(self, table, records: list) -> int:
        """Append a list of dicts to an Iceberg table. Returns row count."""
        import pyarrow as pa
        arrow_table = pa.Table.from_pylist(records, schema=table.schema().as_arrow())
        table.append(arrow_table)
        return len(records)

    def merge_records(self, table_name: str, dataframe, merge_key: str) -> int:
        """
        Upsert records into Iceberg table by merge_key.
        Uses Iceberg's overwrite + append pattern:
          1. Read existing rows matching the merge keys.
          2. Filter out old versions.
          3. Overwrite with combined result.
        Returns the number of rows written.
        """
        ...

    def overwrite_partition(self, table_name, dataframe, partition_column, partition_value) -> int:
        """
        Replace all rows in a specific partition with the given dataframe.
        Uses Iceberg's overwrite_filter for idempotent partition writes.
        """
        table = self.load_table(table_name)
        from pyiceberg.expressions import EqualTo
        table.overwrite(
            dataframe,
            overwrite_filter=EqualTo(partition_column, partition_value),
        )
        return len(dataframe)

    def read_partition(self, table_name, partition_column, partition_value):
        """Read all rows from a specific partition."""
        table = self.load_table(table_name)
        from pyiceberg.expressions import EqualTo
        scan = table.scan(row_filter=EqualTo(partition_column, partition_value))
        return scan.to_arrow().to_pandas()

    def read_new_records(self, table_name, watermark_column, context):
        """
        Read records newer than the last recorded watermark.
        The watermark is stored as Dagster asset metadata from the previous run.
        """
        ...
```

### 3.3 DuckDB Resource

**File**: `orchestrator/resources/duckdb.py`

```python
from dagster import ConfigurableResource
from pydantic import Field
import duckdb


class DuckDBResource(ConfigurableResource):
    """
    Provides DuckDB connections for in-memory analytical processing.
    Can operate in pure in-memory mode or with a persistent database file.
    """

    database_path: str = Field(
        default=":memory:",
        description="Path to DuckDB database file. Use ':memory:' for ephemeral processing.",
    )

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self.database_path)
        # Install and load extensions needed for Iceberg/Parquet
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        return conn
```

### 3.4 Prometheus Resource

**File**: `orchestrator/resources/prometheus.py`

```python
from dagster import ConfigurableResource
from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway
from pydantic import Field


class PrometheusResource(ConfigurableResource):
    """
    Push pipeline metrics to Prometheus Pushgateway for Grafana dashboards.
    """

    pushgateway_url: str = Field(description="Prometheus Pushgateway URL.")
    job_name: str = Field(default="dagster_pipeline", description="Prometheus job label.")

    def push_metric(self, metric_name: str, value: float, labels: dict = None):
        registry = CollectorRegistry()
        gauge = Gauge(metric_name, metric_name, labelnames=list((labels or {}).keys()), registry=registry)
        if labels:
            gauge.labels(**labels).set(value)
        else:
            gauge.set(value)
        push_to_gateway(self.pushgateway_url, job=self.job_name, registry=registry)

    def push_row_count(self, table_name: str, count: int):
        self.push_metric(
            "dagster_table_row_count",
            value=count,
            labels={"table": table_name},
        )

    def push_pipeline_duration(self, asset_name: str, duration_seconds: float):
        self.push_metric(
            "dagster_asset_duration_seconds",
            value=duration_seconds,
            labels={"asset": asset_name},
        )
```

### 3.5 Resource Configuration per Environment

Resources are configured differently for dev, staging, and production by overriding environment variables.

| Resource | Config Key | Dev | Staging | Prod |
|---|---|---|---|---|
| `kafka` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka-staging:9092` | `kafka-prod-1:9092,kafka-prod-2:9092` |
| `kafka` | `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | `http://sr-staging:8081` | `http://sr-prod:8081` |
| `iceberg` | `ICEBERG_CATALOG_URI` | `http://localhost:8181` | `http://iceberg-staging:8181` | `http://iceberg-prod:8181` |
| `iceberg` | `ICEBERG_WAREHOUSE_PATH` | `file:///tmp/warehouse` | `s3://staging-warehouse/` | `s3://prod-warehouse/` |
| `duckdb` | `DUCKDB_DATABASE_PATH` | `:memory:` | `:memory:` | `:memory:` |
| `prometheus` | `PROMETHEUS_PUSHGATEWAY_URL` | `http://localhost:9091` | `http://pushgw-staging:9091` | `http://pushgw-prod:9091` |

An `.env.example` file is committed to the repo. Actual `.env` files are generated per environment and never committed.

---

## 4. Scheduling Strategies

### 4.1 Continuous Ingestion — Bronze Layer (Sensor-Based)

Bronze assets are **not** cron-scheduled. Instead, a Kafka sensor detects new messages and triggers materialization. This achieves near-real-time ingestion with at-most a few seconds of latency between message arrival and Dagster run launch.

```
Trigger: Kafka sensor (see Section 9.1)
Frequency: Continuous polling — sensor evaluates every 10 seconds
Concurrency: At most 1 in-flight Bronze run per topic (enforced via sensor cursor)
```

### 4.2 Micro-Batch — Silver Layer (Every 5 Minutes)

Silver assets run on a tight micro-batch schedule to keep the cleaned data close to real-time.

**File**: `orchestrator/schedules/__init__.py` (partial)

```python
from dagster import ScheduleDefinition, define_asset_job, AssetSelection

silver_assets_job = define_asset_job(
    name="silver_micro_batch_job",
    selection=AssetSelection.groups("silver"),
    description="Micro-batch: transform and clean all Silver assets.",
)

silver_micro_batch_schedule = ScheduleDefinition(
    name="silver_micro_batch_schedule",
    job=silver_assets_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    execution_timezone="America/New_York",
    description="Run silver transforms every 5 minutes during market hours.",
)
```

### 4.3 Daily Batch — Gold Layer (5:00 PM ET)

Gold aggregations run once daily after market close.

**File**: `orchestrator/schedules/daily.py`

```python
from dagster import (
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    AssetSelection,
)

gold_assets_job = define_asset_job(
    name="gold_daily_aggregation_job",
    selection=AssetSelection.groups("gold"),
    description="Daily: compute all Gold aggregation assets for today's partition.",
    partitions_def=daily_partitions,
)

daily_gold_schedule = build_schedule_from_partitioned_job(
    job=gold_assets_job,
    description="Trigger Gold aggregation daily at 5:00 PM ET (after market close).",
    hour_of_day=17,
    minute_of_hour=0,
)
```

Equivalent cron: `0 17 * * 1-5` (weekdays only, 5:00 PM ET).

### 4.4 Weekly — Dimension Table Refresh

Dimensions are refreshed every Sunday at 2:00 AM ET.

**File**: `orchestrator/schedules/weekly.py`

```python
from dagster import ScheduleDefinition, define_asset_job, AssetSelection

dimension_refresh_job = define_asset_job(
    name="dimension_refresh_job",
    selection=AssetSelection.groups("dimensions"),
    description="Weekly: full refresh of all dimension tables with SCD processing.",
)

weekly_dimension_schedule = ScheduleDefinition(
    name="weekly_dimension_schedule",
    job=dimension_refresh_job,
    cron_schedule="0 2 * * 0",  # Sunday 2:00 AM
    execution_timezone="America/New_York",
    description="Refresh dimension tables every Sunday at 2:00 AM ET.",
)
```

### 4.5 Monthly — Historical Recomputation

A monthly job re-materializes the Gold layer for the previous month. This catches any late-arriving corrections in the Silver layer.

**File**: `orchestrator/schedules/monthly.py`

```python
from dagster import (
    schedule,
    RunRequest,
    define_asset_job,
    AssetSelection,
)
from datetime import datetime, timedelta

gold_backfill_job = define_asset_job(
    name="gold_monthly_backfill_job",
    selection=AssetSelection.groups("gold"),
    partitions_def=daily_partitions,
)


@schedule(
    job=gold_backfill_job,
    cron_schedule="0 3 1 * *",  # 1st of each month at 3:00 AM
    execution_timezone="America/New_York",
    description="Monthly recomputation of Gold assets for the prior month.",
)
def monthly_recompute_schedule(context):
    """
    On the 1st of each month, yield RunRequests for each trading day
    of the previous month to recompute Gold aggregations.
    """
    today = context.scheduled_execution_time.date()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    current = last_month_start
    while current <= last_month_end:
        # Skip weekends (no trading)
        if current.weekday() < 5:
            yield RunRequest(
                run_key=f"monthly-recompute-{current.isoformat()}",
                partition_key=current.isoformat(),
                tags={"recompute": "monthly"},
            )
        current += timedelta(days=1)
```

### 4.6 Schedule Summary

| Layer | Schedule | Cron Expression | Timezone | Notes |
|---|---|---|---|---|
| Bronze | Sensor (continuous) | N/A | N/A | Kafka sensor, 10s eval interval |
| Silver | Micro-batch | `*/5 * * * *` | America/New_York | Every 5 minutes |
| Gold | Daily | `0 17 * * 1-5` | America/New_York | After market close, weekdays |
| Dimensions | Weekly | `0 2 * * 0` | America/New_York | Sunday 2 AM |
| Gold (backfill) | Monthly | `0 3 1 * *` | America/New_York | 1st of month, all prior month trading days |

---

## 5. Dependency Management

### 5.1 Asset Dependency Graph

The complete dependency graph flows from left to right:

```
                                       ┌─────────────────────────────┐
                                       │   gold_daily_trading_summary│
                                  ┌──▶ │   (daily partition)         │──┐
                                  │    └─────────────────────────────┘  │
┌──────────────┐   ┌────────────┐ │    ┌─────────────────────────────┐  │  ┌──────────────────┐
│bronze_raw_    │──▶│silver_     │─┤──▶ │   gold_trader_performance   │  ├─▶│gold_market_      │
│trades         │   │trades      │ │    └─────────────────────────────┘  │  │overview          │
└──────────────┘   └────────────┘ │    ┌─────────────────────────────┐  │  └──────────────────┘
                        │         └──▶ │   gold_portfolio_positions   │  │
                        │              └─────────────────────────────┘  │
                        ▼                                               │
                   ┌────────────┐                                       │
                   │silver_     │───────────────────────────────────────┘
                   │trader_     │
                   │activity    │
                   └────────────┘

┌──────────────┐   ┌────────────────────┐
│bronze_raw_    │──▶│silver_orderbook_   │
│orderbook      │   │snapshots           │
└──────────────┘   └────────────────────┘

┌──────────────┐   ┌────────────┐   ┌─────────────────────────────┐
│bronze_raw_    │──▶│silver_     │──▶│   gold_daily_trading_summary│
│marketdata     │   │market_data │   │   (also depends on trades)  │
└──────────────┘   └────────────┘   └─────────────────────────────┘

                   ┌──────────┐
                   │dim_symbol│  (independent — enrichment joins at query time)
                   │dim_trader│
                   │dim_exchange│
                   │dim_time  │
                   │dim_account│
                   └──────────┘
```

### 5.2 Cross-Asset Dependencies via `deps` / `AssetIn`

Dependencies are declared explicitly in asset definitions using `ins` with `AssetIn` or via the `deps` parameter. This ensures Dagster knows the correct execution order and can propagate materialization events downstream.

```python
# Example: gold_daily_trading_summary depends on two silver assets
@asset(
    ins={
        "silver_trades": AssetIn(key=AssetKey(["silver", "silver_trades"])),
        "silver_market_data": AssetIn(key=AssetKey(["silver", "silver_market_data"])),
    },
    ...
)
def gold_daily_trading_summary(context, silver_trades, silver_market_data, ...):
    ...
```

When `silver_trades` is materialized, Dagster records this. The `gold_daily_trading_summary` asset knows it depends on a fresh `silver_trades` and can be triggered accordingly (via schedule or `AutoMaterializePolicy`).

### 5.3 Asset Groups

Assets are grouped by layer for organizational clarity and to allow bulk operations (e.g., "materialize all Silver assets").

| Group Name | Assets | Purpose |
|---|---|---|
| `bronze` | bronze_raw_trades, bronze_raw_orderbook, bronze_raw_marketdata | Raw ingestion |
| `silver` | silver_trades, silver_orderbook_snapshots, silver_market_data, silver_trader_activity | Cleaned + transformed |
| `gold` | gold_daily_trading_summary, gold_trader_performance, gold_market_overview, gold_portfolio_positions | Business aggregations |
| `dimensions` | dim_symbol, dim_trader, dim_exchange, dim_time, dim_account | Reference / SCD tables |

Groups enable selection patterns in jobs:

```python
# Materialize only silver
silver_job = define_asset_job("silver_job", selection=AssetSelection.groups("silver"))

# Materialize everything downstream of bronze
downstream_job = define_asset_job(
    "downstream_job",
    selection=AssetSelection.groups("bronze").downstream(depth=10),
)
```

---

## 6. Retry Logic & Failure Handling

### 6.1 Retry Policy per Layer

Each layer has a tuned retry policy reflecting the expected failure modes.

| Layer | Max Retries | Initial Delay | Backoff | Jitter | Rationale |
|---|---|---|---|---|---|
| Bronze | 5 | 30s | Exponential | Plus/Minus | Kafka broker transient failures, network blips. Higher retry count because upstream is external. |
| Silver | 3 | 60s | Exponential | Plus/Minus | DuckDB OOM or Iceberg lock contention. Less likely to be transient, so fewer retries. |
| Gold | 2 | 120s | Exponential | Plus/Minus | Compute-heavy aggregations. If it fails twice, it likely needs manual investigation. |
| Dimensions | 2 | 60s | Exponential | Plus/Minus | Reference data loads; failures are rare. |

**Code pattern** (applied to every asset via the `retry_policy` parameter):

```python
from dagster import RetryPolicy, Backoff, Jitter

BRONZE_RETRY_POLICY = RetryPolicy(
    max_retries=5,
    delay=30,
    backoff=Backoff.EXPONENTIAL,  # 30s, 60s, 120s, 240s, 480s
    jitter=Jitter.PLUS_MINUS,    # +/- random variance to avoid thundering herd
)
```

### 6.2 Failure Hooks for Alerting

Failure hooks fire when an asset or op fails after all retries are exhausted.

**File**: `orchestrator/hooks/alerting.py`

```python
from dagster import failure_hook, HookContext
import requests


@failure_hook
def slack_alert_on_failure(context: HookContext):
    """
    Send a Slack notification when an asset fails.
    """
    asset_name = context.op.name
    run_id = context.run_id
    error = context.op_exception

    payload = {
        "text": (
            f":red_circle: *Dagster Asset Failed*\n"
            f"*Asset*: `{asset_name}`\n"
            f"*Run ID*: `{run_id}`\n"
            f"*Error*: ```{str(error)[:500]}```\n"
            f"*Link*: {context.instance.dagster_cloud_url}/runs/{run_id}"
        ),
    }
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook_url:
        requests.post(webhook_url, json=payload, timeout=10)


@failure_hook
def email_alert_on_failure(context: HookContext):
    """
    Send an email notification for critical asset failures.
    Reserved for Gold-layer failures that affect downstream reporting.
    """
    ...
```

Hooks are attached to jobs:

```python
from orchestrator.hooks.alerting import slack_alert_on_failure

gold_assets_job = define_asset_job(
    name="gold_daily_aggregation_job",
    selection=AssetSelection.groups("gold"),
    hooks={slack_alert_on_failure},
)
```

### 6.3 Run Failure Sensor

A sensor that watches for any failed run and triggers a centralized alerting flow.

```python
from dagster import run_failure_sensor, RunFailureSensorContext

@run_failure_sensor
def notify_on_run_failure(context: RunFailureSensorContext):
    """
    Fires for ANY failed run across all jobs. Sends to the ops Slack channel
    and increments a Prometheus failure counter.
    """
    run = context.dagster_run
    error = context.failure_event.message

    # Post to Slack
    ...

    # Push failure metric to Prometheus
    ...
```

### 6.4 Partial Asset Materialization

By default, if one asset in a multi-asset job fails, Dagster does **not** roll back already-succeeded assets. This is the desired behavior:

- If `silver_trades` succeeds but `silver_orderbook_snapshots` fails, the trades data is still available.
- The failed asset is marked as failed and will be retried independently.
- Downstream assets that depend on the failed asset will not be triggered.

This is achieved by using `AssetSelection` in jobs rather than a single monolithic graph.

### 6.5 Op-Level vs Run-Level Retry

| Strategy | When to Use | Configuration |
|---|---|---|
| **Op-level retry** (`RetryPolicy`) | Transient failures within a single asset (Kafka timeout, network glitch). The asset function is re-invoked. | Set via `retry_policy` on `@asset`. |
| **Run-level retry** | Entire run fails due to infrastructure issue (OOM, daemon crash). The full run is re-launched. | Configure in `dagster.yaml` under `run_retries: max_retries: 1`. |

**Recommendation**: Use op-level retry as the primary mechanism. Run-level retry is a safety net and should be limited to 1 attempt to avoid runaway retries.

```yaml
# In dagster.yaml
run_retries:
  max_retries: 1
```

---

## 7. Backfilling Historical Data

### 7.1 Partitioned Assets with `DailyPartitionsDefinition`

Gold assets use `DailyPartitionsDefinition` so that each trading day is an independently materializable partition. This enables targeted backfills without reprocessing the entire history.

**File**: `orchestrator/partitions/daily.py`

```python
from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

# Used by Gold assets — one partition per calendar day.
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="America/New_York",
    end_offset=0,  # Do not create future partitions
)

# Used by monthly recomputation jobs.
monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2024-01-01",
    timezone="America/New_York",
)
```

### 7.2 Backfill Jobs

Backfills are launched from the Dagster UI or via CLI by selecting a date range on partitioned assets.

**UI workflow:**
1. Navigate to the asset in the Dagster UI.
2. Click "Materialize" -> "Launch Backfill".
3. Select the date range (e.g., 2024-01-01 to 2024-06-30).
4. Dagster queues one run per partition day.

**CLI workflow:**
```bash
dagster asset backfill \
  --asset-key gold/gold_daily_trading_summary \
  --partition-range 2024-01-01...2024-06-30 \
  --nonce backfill-q1q2-2024
```

### 7.3 Backfill Prioritization and Resource Limits

To prevent backfills from consuming all resources:

```yaml
# In dagster.yaml — limit concurrent runs from the queue
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5
    tag_concurrency_limits:
      - key: "dagster/backfill"
        limit: 3  # At most 3 concurrent backfill runs
      - key: "layer"
        value: "gold"
        limit: 2  # At most 2 concurrent Gold runs
```

Backfill runs are automatically tagged with `dagster/backfill`. Additional concurrency controls are enforced via tag-based limits.

### 7.4 Historical Data Generation in trade-generator

The trade-generator service (data source) supports a `--historical` mode that produces trade events for past dates. During backfill:

1. Trigger the trade-generator in historical mode for the target date range.
2. Historical events are published to the same Kafka topics with correct timestamps.
3. Bronze assets ingest them (sensor-triggered or manual).
4. Silver transforms run on the micro-batch schedule.
5. Gold backfill job is launched for the date range.

### 7.5 Idempotent Backfill Operations

Backfills are safe to re-run because:

- **Gold assets** use `overwrite_partition()` — recomputing a day replaces the previous result.
- **Silver assets** use merge-by-primary-key — re-processing the same records produces the same result.
- **Bronze assets** use Kafka offset tracking — re-consuming already-committed offsets is a no-op (the consumer starts after the last committed offset).

---

## 8. Idempotency

Idempotency is critical for reliability. Every asset must produce the same result regardless of how many times it is executed for the same input.

### 8.1 Strategy per Layer

| Layer | Strategy | Mechanism | Details |
|---|---|---|---|
| **Bronze** | Kafka offset tracking + exactly-once write | Offsets committed after Iceberg write; consumer starts from last committed offset. | If the Dagster run fails after Iceberg write but before offset commit, the next run re-reads the same messages and attempts to append. To prevent duplicates, each record includes an `ingestion_run_id` and a dedup step in Silver handles any duplicates. |
| **Silver** | Merge by primary key (upsert) | `MERGE INTO silver.trades USING new_records ON trade_id = trade_id WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` | Reprocessing the same Bronze records results in the same Silver rows because the merge is keyed on business primary keys. |
| **Gold** | Full recompute of partition (overwrite) | `iceberg.overwrite(df, overwrite_filter=EqualTo("trading_date", partition_date))` | Each partition is independently recomputed from Silver. The entire partition is atomically replaced. |

### 8.2 Iceberg Overwrite-by-Filter for Partition Idempotency

Iceberg's `overwrite` operation with a filter expression atomically replaces all rows matching the filter. This is the foundation of Gold-layer idempotency.

```python
from pyiceberg.expressions import EqualTo

def overwrite_partition(table, dataframe, partition_column, partition_value):
    """
    Atomically replace all rows where partition_column = partition_value
    with the contents of dataframe.
    """
    table.overwrite(
        dataframe,
        overwrite_filter=EqualTo(partition_column, partition_value),
    )
```

This operation is:
- **Atomic**: Readers see either the old data or the new data, never a partial state.
- **Idempotent**: Running it twice with the same input produces the same result.
- **Scoped**: Only the targeted partition is affected; other partitions are untouched.

### 8.3 Run Tags for Deduplication

Dagster `run_key` in `RunRequest` ensures that the same logical run is not launched twice. This prevents duplicate runs from sensors or schedules that fire concurrently.

```python
# In a sensor:
yield RunRequest(
    run_key=f"bronze-trades-offset-{current_offset}",
    ...
)
# If the sensor fires again with the same offset, Dagster skips the duplicate.
```

---

## 9. Sensors

### 9.1 Kafka Topic Sensor

Polls Kafka for new messages and triggers Bronze asset materialization.

**File**: `orchestrator/sensors/kafka_sensor.py`

```python
from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    DefaultSensorStatus,
    define_asset_job,
    AssetSelection,
)

bronze_trades_job = define_asset_job(
    name="bronze_trades_ingestion",
    selection=AssetSelection.keys(["bronze", "bronze_raw_trades"]),
)


@sensor(
    job=bronze_trades_job,
    minimum_interval_seconds=10,
    default_status=DefaultSensorStatus.RUNNING,
    description="Polls Kafka 'raw.trades' topic for new messages. "
                "Triggers bronze_raw_trades materialization when messages are available.",
)
def kafka_trades_sensor(context: SensorEvaluationContext, kafka: KafkaResource):
    """
    1. Create an admin client to query topic offsets.
    2. Compare current high-water mark with the last committed consumer offset.
    3. If new messages exist, yield a RunRequest.
    4. Store the checked offset as the sensor cursor to avoid re-triggering.
    """
    from confluent_kafka.admin import AdminClient

    # Get current high-water mark for the topic
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    topic = "raw.trades"

    # Get latest offset
    latest_offset = kafka.get_topic_high_watermark(topic)

    # Compare with cursor (last offset we triggered a run for)
    last_processed_offset = int(context.cursor) if context.cursor else 0

    if latest_offset <= last_processed_offset:
        yield SkipReason(f"No new messages on {topic} (offset {latest_offset})")
        return

    context.update_cursor(str(latest_offset))

    yield RunRequest(
        run_key=f"bronze-trades-{latest_offset}",
        tags={
            "kafka_topic": topic,
            "offset_start": str(last_processed_offset),
            "offset_end": str(latest_offset),
            "layer": "bronze",
        },
    )
```

Separate sensors exist for `raw.orderbook` and `raw.marketdata` topics, following the same pattern.

### 9.2 File Sensor (MinIO)

Watches for new data files in MinIO (used for batch file ingestion, e.g., reference data uploads).

```python
from dagster import sensor, RunRequest

@sensor(
    job=dimension_refresh_job,
    minimum_interval_seconds=60,
    description="Watch MinIO bucket for new reference data files.",
)
def minio_file_sensor(context):
    """
    List objects in the 's3://reference-data/' prefix.
    Trigger dimension refresh when new files appear.
    """
    import boto3

    s3 = boto3.client("s3", endpoint_url=os.environ["MINIO_ENDPOINT"])
    response = s3.list_objects_v2(Bucket="reference-data", Prefix="incoming/")

    last_key = context.cursor or ""
    new_files = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"] > last_key
    ]

    if not new_files:
        return

    context.update_cursor(max(new_files))
    yield RunRequest(
        run_key=f"ref-data-{max(new_files)}",
        tags={"trigger": "file_sensor", "file": max(new_files)},
    )
```

### 9.3 Run Status Sensor

Triggers downstream processing when an upstream run succeeds.

```python
from dagster import run_status_sensor, DagsterRunStatus, RunRequest

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=silver_assets_job,
    description="When a Bronze ingestion run succeeds, immediately trigger Silver transforms.",
)
def bronze_success_trigger_silver(context):
    """
    Skip the 5-minute schedule wait and immediately kick off Silver
    processing when Bronze data lands.
    """
    dagster_run = context.dagster_run
    if "bronze" in dagster_run.job_name:
        yield RunRequest(
            run_key=f"silver-after-bronze-{dagster_run.run_id}",
            tags={"triggered_by": dagster_run.run_id, "layer": "silver"},
        )
```

### 9.4 Freshness Policy Sensors

Freshness policies are declared on assets (e.g., `FreshnessPolicy(maximum_lag_minutes=15)` on Silver). Dagster's built-in freshness evaluation automatically monitors these. When an asset's latest materialization is older than the allowed lag, the Dagster UI shows it as "overdue" and an alert can be configured.

```python
# On the asset definition:
@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=15),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ...
)
def silver_trades(...):
    ...
```

For external alerting beyond the UI, a custom sensor can check freshness:

```python
@sensor(minimum_interval_seconds=300)
def freshness_alert_sensor(context):
    """
    Check freshness of critical assets every 5 minutes.
    Alert if any asset is stale beyond its allowed lag.
    """
    critical_assets = [
        ("silver", "silver_trades", 15),
        ("gold", "gold_daily_trading_summary", 120),
    ]

    for prefix, name, max_lag_min in critical_assets:
        record = context.instance.get_latest_materialization_event(
            AssetKey([prefix, name])
        )
        if record:
            age_minutes = (datetime.now() - record.timestamp).total_seconds() / 60
            if age_minutes > max_lag_min:
                # Send alert
                ...
```

---

## 10. Dagster Ops & Jobs (Non-Asset Workflows)

Some workflows do not produce assets but perform operational tasks. These are modeled as traditional Dagster ops and jobs.

### 10.1 Maintenance Job — Iceberg Compaction & Cleanup

**File**: `orchestrator/jobs/maintenance.py`

```python
from dagster import job, op, In, Out, RetryPolicy


@op(
    description="Run Iceberg table compaction to merge small files.",
    retry_policy=RetryPolicy(max_retries=2, delay=60),
)
def compact_iceberg_tables(context, iceberg: IcebergResource):
    """
    For each managed Iceberg table:
      1. Run rewrite_data_files() to compact small Parquet files.
      2. Run expire_snapshots() to remove old snapshots (retain 7 days).
      3. Run remove_orphan_files() to clean up unreferenced files.
    """
    tables = [
        "bronze.raw_trades", "bronze.raw_orderbook", "bronze.raw_marketdata",
        "silver.trades", "silver.orderbook_snapshots", "silver.market_data",
        "gold.daily_trading_summary", "gold.trader_performance",
        "gold.market_overview", "gold.portfolio_positions",
    ]

    for table_name in tables:
        context.log.info(f"Compacting {table_name}...")
        table = iceberg.load_table(table_name)
        # Compact small files (target file size: 256 MB)
        # Expire snapshots older than 7 days
        # Remove orphan files
        ...


@op(
    description="Remove expired Dagster run records older than 30 days.",
)
def cleanup_dagster_runs(context):
    """
    Prune old run records from the Dagster instance database to prevent
    unbounded storage growth.
    """
    ...


@job(
    description="Weekly maintenance: compact Iceberg tables, expire snapshots, "
                "clean up old Dagster runs.",
    tags={"layer": "maintenance"},
)
def maintenance_job():
    cleanup_dagster_runs()
    compact_iceberg_tables()
```

**Schedule**: Weekly, Sunday 4:00 AM ET.

```python
maintenance_schedule = ScheduleDefinition(
    name="weekly_maintenance",
    job=maintenance_job,
    cron_schedule="0 4 * * 0",
    execution_timezone="America/New_York",
)
```

### 10.2 Data Quality Job

**File**: `orchestrator/jobs/data_quality.py`

```python
from dagster import job, op


@op(description="Run Great Expectations checkpoint for Silver tables.")
def run_silver_quality_checks(context):
    """
    Execute the Great Expectations checkpoint 'silver_checkpoint' which
    validates:
      - silver.trades: trade_id not null, price > 0, quantity >= 0
      - silver.market_data: high >= low, volume >= 0
      - silver.orderbook_snapshots: best_bid < best_ask
    Returns a validation result that is logged as Dagster metadata.
    """
    ...


@op(description="Run Great Expectations checkpoint for Gold tables.")
def run_gold_quality_checks(context):
    """
    Execute the 'gold_checkpoint':
      - gold.daily_trading_summary: all prices > 0, trade_count > 0
      - gold.trader_performance: win_rate between 0 and 1
    """
    ...


@job(
    description="Run all data quality checks across Silver and Gold layers.",
    tags={"layer": "quality"},
)
def data_quality_job():
    run_silver_quality_checks()
    run_gold_quality_checks()
```

**Schedule**: Daily after Gold aggregation completes (triggered by run_status_sensor on Gold success), or standalone at 6:00 PM ET.

### 10.3 Export Job

**File**: `orchestrator/jobs/export.py`

```python
from dagster import job, op


@op(description="Export daily trading summary to CSV for external consumers.")
def export_daily_summary(context, iceberg: IcebergResource):
    """
    Read gold.daily_trading_summary for yesterday's partition and write
    to a CSV file in the MinIO 'exports' bucket.
    """
    ...


@op(description="Export trader performance report to Parquet for BI tools.")
def export_trader_report(context, iceberg: IcebergResource):
    """
    Read gold.trader_performance for the last 30 days and write a
    consolidated Parquet file to MinIO 'exports' bucket.
    """
    ...


@job(
    description="Export Gold data for external consumption (CSV, Parquet to MinIO).",
    tags={"layer": "export"},
)
def export_job():
    export_daily_summary()
    export_trader_report()
```

---

## 11. Implementation Steps & File Layout

### 11.1 Complete File Tree

```
services/dagster-orchestrator/
├── setup.py                            # Python package definition
├── setup.cfg                           # Package metadata
├── pyproject.toml                      # Build system config
├── dagster.yaml                        # Dagster instance configuration
├── workspace.yaml                      # Code location pointing to orchestrator module
├── Dockerfile                          # Container image for Dagster services
├── docker-compose.dagster.yaml         # Compose: webserver + daemon + postgres
├── .env.example                        # Template environment variables
├── orchestrator/
│   ├── __init__.py                     # Package init (can re-export Definitions)
│   ├── definitions.py                  # Central Definitions object
│   ├── assets/
│   │   ├── __init__.py                 # Exports all asset lists
│   │   ├── bronze.py                   # bronze_raw_trades, bronze_raw_orderbook, bronze_raw_marketdata
│   │   ├── silver.py                   # silver_trades, silver_orderbook_snapshots, silver_market_data, silver_trader_activity
│   │   ├── gold.py                     # gold_daily_trading_summary, gold_trader_performance, gold_market_overview, gold_portfolio_positions
│   │   └── dimensions.py              # dim_symbol, dim_trader, dim_exchange, dim_time, dim_account
│   ├── resources/
│   │   ├── __init__.py                 # Exports resource dict
│   │   ├── kafka.py                    # KafkaResource (ConfigurableResource)
│   │   ├── iceberg.py                  # IcebergResource (ConfigurableResource)
│   │   ├── duckdb.py                   # DuckDBResource (ConfigurableResource)
│   │   └── prometheus.py               # PrometheusResource (ConfigurableResource)
│   ├── sensors/
│   │   ├── __init__.py                 # Exports sensor list
│   │   ├── kafka_sensor.py             # kafka_trades_sensor, kafka_orderbook_sensor, kafka_marketdata_sensor
│   │   ├── freshness_sensor.py         # freshness_alert_sensor
│   │   └── run_status_sensor.py        # bronze_success_trigger_silver, run_failure_sensor
│   ├── schedules/
│   │   ├── __init__.py                 # Exports schedule list
│   │   ├── daily.py                    # daily_gold_schedule, silver_micro_batch_schedule
│   │   ├── weekly.py                   # weekly_dimension_schedule, weekly_maintenance_schedule
│   │   └── monthly.py                  # monthly_recompute_schedule
│   ├── jobs/
│   │   ├── __init__.py                 # Exports job list
│   │   ├── maintenance.py              # maintenance_job (compact, expire, cleanup)
│   │   ├── data_quality.py             # data_quality_job (Great Expectations)
│   │   └── export.py                   # export_job (CSV/Parquet to MinIO)
│   ├── partitions/
│   │   ├── __init__.py                 # Exports partition definitions
│   │   └── daily.py                    # daily_partitions, monthly_partitions
│   ├── hooks/
│   │   ├── __init__.py
│   │   └── alerting.py                 # slack_alert_on_failure, email_alert_on_failure
│   └── utils/
│       ├── __init__.py
│       ├── kafka_helpers.py            # Offset management, topic watermark queries
│       └── iceberg_helpers.py          # Merge logic, partition overwrite, watermark tracking
└── tests/
    ├── __init__.py
    ├── conftest.py                     # Shared pytest fixtures (mock resources, test catalog)
    ├── test_bronze_assets.py           # Unit tests for Bronze asset functions
    ├── test_silver_assets.py           # Unit tests for Silver transformation logic
    ├── test_gold_assets.py             # Unit tests for Gold aggregation logic
    ├── test_dimension_assets.py        # Unit tests for SCD processing
    ├── test_sensors.py                 # Sensor evaluation tests
    ├── test_schedules.py              # Schedule validation tests
    ├── test_resources.py               # Resource configuration tests
    └── test_jobs.py                    # Job graph structure tests
```

### 11.2 Implementation Order

The implementation should proceed in this order, each step building on the previous.

| Phase | Step | Description | Depends On |
|---|---|---|---|
| **Phase 1: Foundation** | 1.1 | Create project structure, `setup.py`, `pyproject.toml` | — |
| | 1.2 | Write `dagster.yaml` and `workspace.yaml` | 1.1 |
| | 1.3 | Write `Dockerfile` and `docker-compose.dagster.yaml` | 1.2 |
| | 1.4 | Implement `definitions.py` skeleton (empty asset/resource lists) | 1.1 |
| | 1.5 | Verify Dagster webserver starts and shows empty UI | 1.3, 1.4 |
| **Phase 2: Resources** | 2.1 | Implement `KafkaResource` with consumer/producer helpers | 1.5 |
| | 2.2 | Implement `IcebergResource` with catalog, read, write, merge methods | 1.5 |
| | 2.3 | Implement `DuckDBResource` | 1.5 |
| | 2.4 | Implement `PrometheusResource` | 1.5 |
| | 2.5 | Write unit tests for all resources | 2.1–2.4 |
| **Phase 3: Bronze Assets** | 3.1 | Implement `bronze_raw_trades` | 2.1, 2.2 |
| | 3.2 | Implement `bronze_raw_orderbook` | 2.1, 2.2 |
| | 3.3 | Implement `bronze_raw_marketdata` | 2.1, 2.2 |
| | 3.4 | Write Bronze unit tests | 3.1–3.3 |
| | 3.5 | Implement Kafka sensors for all three topics | 3.1–3.3 |
| **Phase 4: Silver Assets** | 4.1 | Implement `silver_trades` (dedup, type casting, null handling) | 3.1, 2.3 |
| | 4.2 | Implement `silver_orderbook_snapshots` | 3.2, 2.3 |
| | 4.3 | Implement `silver_market_data` | 3.3, 2.3 |
| | 4.4 | Implement `silver_trader_activity` | 4.1 |
| | 4.5 | Write Silver unit tests | 4.1–4.4 |
| | 4.6 | Implement micro-batch schedule (every 5 min) | 4.1–4.4 |
| **Phase 5: Gold Assets** | 5.1 | Implement `gold_daily_trading_summary` with partition definition | 4.1, 4.3 |
| | 5.2 | Implement `gold_trader_performance` | 4.1, 4.4 |
| | 5.3 | Implement `gold_market_overview` | 5.1 |
| | 5.4 | Implement `gold_portfolio_positions` | 4.1 |
| | 5.5 | Write Gold unit tests | 5.1–5.4 |
| | 5.6 | Implement daily Gold schedule | 5.1–5.4 |
| **Phase 6: Dimensions** | 6.1 | Implement SCD Type 2 logic in `dim_symbol` and `dim_trader` | 2.2, 2.3 |
| | 6.2 | Implement SCD Type 1 in `dim_exchange` | 2.2, 2.3 |
| | 6.3 | Implement static `dim_time` generator | 2.2, 2.3 |
| | 6.4 | Implement SCD Type 3 in `dim_account` | 2.2, 2.3 |
| | 6.5 | Write dimension unit tests | 6.1–6.4 |
| | 6.6 | Implement weekly dimension schedule | 6.1–6.4 |
| **Phase 7: Operations** | 7.1 | Implement `maintenance_job` (compaction, snapshot expiry) | 2.2 |
| | 7.2 | Implement `data_quality_job` (Great Expectations integration) | 4.1–4.4, 5.1–5.4 |
| | 7.3 | Implement `export_job` | 5.1–5.4 |
| | 7.4 | Implement failure hooks (Slack, email) | — |
| | 7.5 | Implement run_status_sensor and freshness_sensor | 7.4 |
| **Phase 8: Backfill & Polish** | 8.1 | Implement monthly recompute schedule | 5.6 |
| | 8.2 | Configure tag-based concurrency limits for backfills | 8.1 |
| | 8.3 | End-to-end integration test (Bronze -> Silver -> Gold) | All |
| | 8.4 | Document operational runbooks (backfill, recovery, scaling) | All |

---

## 12. Testing Strategy

### 12.1 Unit Tests for Asset Computation Functions

Each asset's transformation logic is tested in isolation using mock resources. Dagster's `build_asset_context` provides a test-friendly execution context.

**File**: `tests/conftest.py`

```python
import pytest
from dagster import build_asset_context
from unittest.mock import MagicMock
import duckdb
import pandas as pd


@pytest.fixture
def mock_kafka():
    """Mock Kafka resource that returns canned records."""
    resource = MagicMock()
    resource.consume_batch.return_value = [
        {
            "trade_id": "T001",
            "symbol": "AAPL",
            "price": 150.25,
            "quantity": 100,
            "side": "BUY",
            "trader_id": "TR001",
            "exchange": "NYSE",
            "trade_timestamp": "2024-06-15T10:30:00Z",
            "event_timestamp": "2024-06-15T10:30:01Z",
        },
        # duplicate with same trade_id but later event_timestamp
        {
            "trade_id": "T001",
            "symbol": "AAPL",
            "price": 150.30,
            "quantity": 100,
            "side": "BUY",
            "trader_id": "TR001",
            "exchange": "NYSE",
            "trade_timestamp": "2024-06-15T10:30:00Z",
            "event_timestamp": "2024-06-15T10:30:05Z",
        },
    ]
    return resource


@pytest.fixture
def mock_iceberg():
    """Mock Iceberg resource with in-memory tracking."""
    resource = MagicMock()
    resource.append_records.return_value = 2
    resource.merge_records.return_value = 1  # 1 unique after dedup
    return resource


@pytest.fixture
def test_duckdb():
    """Real DuckDB in-memory connection for transform testing."""
    conn = duckdb.connect(":memory:")
    return conn


@pytest.fixture
def asset_context():
    """Standard Dagster asset context for unit tests."""
    return build_asset_context()
```

**File**: `tests/test_bronze_assets.py`

```python
from orchestrator.assets.bronze import bronze_raw_trades
from dagster import build_asset_context


def test_bronze_raw_trades_ingests_records(mock_kafka, mock_iceberg):
    """Verify bronze_raw_trades consumes from Kafka and writes to Iceberg."""
    context = build_asset_context(
        resources={"kafka": mock_kafka, "iceberg": mock_iceberg}
    )

    result = bronze_raw_trades(context)
    assert result.metadata["row_count"].value == 2
    mock_iceberg.append_records.assert_called_once()
    mock_kafka.commit_offsets.assert_called_once()


def test_bronze_raw_trades_skips_empty_topic(mock_kafka, mock_iceberg):
    """Verify no-op when Kafka topic has no new messages."""
    mock_kafka.consume_batch.return_value = []
    context = build_asset_context(
        resources={"kafka": mock_kafka, "iceberg": mock_iceberg}
    )

    result = bronze_raw_trades(context)
    assert result.metadata["row_count"].value == 0
    mock_iceberg.append_records.assert_not_called()
```

**File**: `tests/test_silver_assets.py`

```python
from orchestrator.assets.silver import silver_trades
from dagster import build_asset_context
import pandas as pd


def test_silver_trades_deduplicates(mock_iceberg, test_duckdb):
    """Verify deduplication by trade_id keeps the latest event_timestamp."""
    # Provide bronze data with duplicates
    mock_iceberg.read_new_records.return_value = pd.DataFrame([
        {"trade_id": "T001", "symbol": "AAPL", "price": 150.25,
         "quantity": 100, "side": "BUY", "trader_id": "TR001",
         "exchange": "NYSE", "trade_timestamp": "2024-06-15T10:30:00Z",
         "event_timestamp": "2024-06-15T10:30:01Z"},
        {"trade_id": "T001", "symbol": "AAPL", "price": 150.30,
         "quantity": 100, "side": "BUY", "trader_id": "TR001",
         "exchange": "NYSE", "trade_timestamp": "2024-06-15T10:30:00Z",
         "event_timestamp": "2024-06-15T10:30:05Z"},
    ])

    context = build_asset_context(
        resources={"iceberg": mock_iceberg, "duckdb": test_duckdb}
    )

    result = silver_trades(context, bronze_raw_trades=None)
    # Should merge 1 record (latest event_timestamp wins)
    assert result.metadata["dedup_removed"].value == 1


def test_silver_trades_drops_null_trade_id(mock_iceberg, test_duckdb):
    """Verify rows with NULL trade_id are dropped."""
    mock_iceberg.read_new_records.return_value = pd.DataFrame([
        {"trade_id": None, "symbol": "AAPL", "price": 150.0,
         "quantity": 100, "side": "BUY", "trader_id": "TR001",
         "exchange": "NYSE", "trade_timestamp": "2024-06-15T10:30:00Z",
         "event_timestamp": "2024-06-15T10:30:01Z"},
    ])

    context = build_asset_context(
        resources={"iceberg": mock_iceberg, "duckdb": test_duckdb}
    )

    result = silver_trades(context, bronze_raw_trades=None)
    assert result.metadata["row_count"].value == 0
```

### 12.2 Integration Tests with Test Resources

Integration tests use real DuckDB and a lightweight Iceberg catalog (SQLite-backed) to verify the full transform pipeline.

```python
# tests/test_integration.py

import pytest
from dagster import materialize
from orchestrator.assets.bronze import bronze_raw_trades
from orchestrator.assets.silver import silver_trades
from orchestrator.resources.iceberg import IcebergResource
from orchestrator.resources.duckdb import DuckDBResource


@pytest.fixture
def test_resources():
    return {
        "kafka": MockKafkaWithTestData(),
        "iceberg": IcebergResource(
            catalog_uri="sqlite:///tmp/test_catalog.db",
            warehouse_path="file:///tmp/test_warehouse",
        ),
        "duckdb": DuckDBResource(database_path=":memory:"),
        "prometheus": MockPrometheus(),
    }


def test_bronze_to_silver_pipeline(test_resources):
    """End-to-end: Bronze ingestion followed by Silver transformation."""
    result = materialize(
        assets=[bronze_raw_trades, silver_trades],
        resources=test_resources,
    )
    assert result.success
    # Verify Silver table has expected row count
    iceberg = test_resources["iceberg"]
    table = iceberg.load_table("silver.trades")
    df = table.scan().to_arrow().to_pandas()
    assert len(df) > 0
    assert df["trade_id"].is_unique
```

### 12.3 Dagster Built-in Testing Utilities

| Utility | Purpose | Usage |
|---|---|---|
| `build_asset_context()` | Create a mock `AssetExecutionContext` for unit testing asset functions | Pass mock resources, partition keys |
| `materialize()` | Execute a set of assets in-process for integration tests | Verifies dependency resolution and execution order |
| `build_sensor_context()` | Test sensor evaluation logic without running the daemon | Verify correct RunRequest generation |
| `build_schedule_context()` | Test schedule tick evaluation | Verify correct partition selection |
| `validate_run_config()` | Validate that a job's run config is structurally correct | Catch config errors before runtime |

**Sensor test example:**

```python
from dagster import build_sensor_context
from orchestrator.sensors.kafka_sensor import kafka_trades_sensor


def test_kafka_sensor_yields_run_on_new_messages(mock_kafka):
    """Sensor should yield a RunRequest when new messages exist."""
    mock_kafka.get_topic_high_watermark.return_value = 1000
    context = build_sensor_context(cursor="500")

    result = kafka_trades_sensor.evaluate_tick(context)
    assert len(result.run_requests) == 1
    assert result.cursor == "1000"


def test_kafka_sensor_skips_when_no_new_messages(mock_kafka):
    """Sensor should skip when offset has not advanced."""
    mock_kafka.get_topic_high_watermark.return_value = 500
    context = build_sensor_context(cursor="500")

    result = kafka_trades_sensor.evaluate_tick(context)
    assert len(result.run_requests) == 0
    assert len(result.skip_messages) == 1
```

### 12.4 Test Execution

```bash
# Run all orchestrator tests
cd services/dagster-orchestrator
pytest tests/ -v --tb=short

# Run with coverage
pytest tests/ --cov=orchestrator --cov-report=html

# Run only unit tests (fast)
pytest tests/ -v -m "not integration"

# Run only integration tests
pytest tests/ -v -m "integration"
```

### 12.5 CI Integration

Tests run as part of the monorepo CI pipeline:

```yaml
# In .github/workflows/ci.yaml (relevant section)
dagster-tests:
  runs-on: ubuntu-latest
  services:
    postgres:
      image: postgres:16-alpine
      env:
        POSTGRES_USER: dagster
        POSTGRES_PASSWORD: test
        POSTGRES_DB: dagster
      ports:
        - 5432:5432
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v5
      with:
        python-version: "3.11"
    - run: pip install -e "services/dagster-orchestrator[dev]"
    - run: pytest services/dagster-orchestrator/tests/ -v --tb=short
```

---

## Appendix A: Environment Variables Reference

| Variable | Description | Example |
|---|---|---|
| `DAGSTER_PG_HOST` | PostgreSQL host for Dagster storage | `dagster-postgres` |
| `DAGSTER_PG_PORT` | PostgreSQL port | `5432` |
| `DAGSTER_PG_USER` | PostgreSQL username | `dagster` |
| `DAGSTER_PG_PASSWORD` | PostgreSQL password | `dagster_secret` |
| `DAGSTER_PG_DB` | PostgreSQL database name | `dagster` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | Confluent Schema Registry URL | `http://localhost:8081` |
| `ICEBERG_CATALOG_URI` | Iceberg REST catalog URI | `http://localhost:8181` |
| `ICEBERG_WAREHOUSE_PATH` | Iceberg warehouse root path | `s3://warehouse/` |
| `DUCKDB_DATABASE_PATH` | DuckDB database path | `:memory:` |
| `PROMETHEUS_PUSHGATEWAY_URL` | Prometheus Pushgateway URL | `http://localhost:9091` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for alerts | `https://hooks.slack.com/...` |
| `MINIO_ENDPOINT` | MinIO S3-compatible endpoint | `http://localhost:9000` |

## Appendix B: Key Dagster Concepts Mapping

| Dagster Concept | Our Usage |
|---|---|
| **Software-Defined Asset** | Every Bronze, Silver, Gold, and Dimension table |
| **Resource** | Kafka, Iceberg, DuckDB, Prometheus connections |
| **Sensor** | Kafka topic polling, file arrival detection, run status reactions |
| **Schedule** | Cron-based triggers for Silver (5min), Gold (daily), Dimensions (weekly) |
| **Partition** | Daily partitions on Gold assets for targeted backfills |
| **RetryPolicy** | Per-layer retry configuration on assets |
| **FreshnessPolicy** | SLA monitoring on Silver (15min) and Gold (120min) assets |
| **Asset Group** | bronze, silver, gold, dimensions — for logical organization |
| **Job** | Non-asset workflows (maintenance, quality, export) |
| **Hook** | Slack/email alerting on failure |
| **Op** | Individual steps within non-asset jobs |
| **Run Coordinator** | QueuedRunCoordinator with concurrency limits for backfills |
