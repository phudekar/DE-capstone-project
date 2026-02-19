# 04 - Storage & Data Modeling: Implementation Plan

## Table of Contents

1. [Apache Iceberg Setup](#1-apache-iceberg-setup)
2. [Data Warehouse Architecture (Medallion / Lakehouse)](#2-data-warehouse-architecture-medallion--lakehouse)
3. [Dimensional Data Model](#3-dimensional-data-model)
4. [SCD Implementation Details](#4-scd-implementation-details)
5. [Partitioning Strategy](#5-partitioning-strategy)
6. [Table Maintenance](#6-table-maintenance)
7. [Schema Evolution](#7-schema-evolution)
8. [Implementation Steps](#8-implementation-steps)
9. [Testing Strategy](#9-testing-strategy)

---

## 1. Apache Iceberg Setup

### 1.1 Infrastructure Components

The storage layer relies on three core infrastructure services running in Docker:

| Component             | Image / Tool                      | Purpose                                   |
|-----------------------|-----------------------------------|-------------------------------------------|
| Iceberg REST Catalog  | `tabulario/iceberg-rest:latest`   | Central metadata catalog for all tables   |
| MinIO                 | `minio/minio:latest`              | S3-compatible local object storage        |
| MinIO Client (mc)     | `minio/mc:latest`                 | One-time bucket initialization            |

### 1.2 Docker Compose Configuration

File: `infra/docker/docker-compose.storage.yml`

```yaml
version: "3.9"

services:
  # ---------------------------------------------------------------
  # MinIO — S3-compatible object storage
  # ---------------------------------------------------------------
  minio:
    image: minio/minio:latest
    container_name: de-minio
    hostname: minio
    ports:
      - "9000:9000"    # S3 API
      - "9001:9001"    # Console UI
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: admin_secret
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ---------------------------------------------------------------
  # MinIO Client — create the warehouse bucket on first boot
  # ---------------------------------------------------------------
  minio-init:
    image: minio/mc:latest
    container_name: de-minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
        mc alias set local http://minio:9000 admin admin_secret &&
        mc mb --ignore-existing local/warehouse &&
        mc mb --ignore-existing local/warehouse-staging &&
        mc anonymous set download local/warehouse &&
        echo 'MinIO bucket initialization complete.'
      "

  # ---------------------------------------------------------------
  # Iceberg REST Catalog
  # ---------------------------------------------------------------
  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    container_name: de-iceberg-rest
    hostname: iceberg-rest
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: admin_secret
      AWS_REGION: us-east-1
    depends_on:
      minio:
        condition: service_healthy

volumes:
  minio_data:
    driver: local
```

### 1.3 Iceberg REST Catalog Configuration

| Setting                  | Value                                   | Notes                                      |
|--------------------------|-----------------------------------------|--------------------------------------------|
| Catalog URI              | `http://localhost:8181`                  | REST endpoint for PyIceberg / Spark        |
| Catalog type             | `rest`                                  | Supports multi-engine (Flink, Spark, etc.) |
| Warehouse location       | `s3://warehouse/`                       | Root path inside MinIO                     |
| FileIO implementation    | `org.apache.iceberg.aws.s3.S3FileIO`   | S3-compatible file operations              |
| S3 endpoint              | `http://minio:9000`                     | MinIO S3 API                               |
| Path-style access        | `true`                                  | Required for MinIO                         |

### 1.4 PyIceberg Client Configuration

File: `libs/data-models/catalog.py`

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "rest",
    **{
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "admin_secret",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    },
)
```

The same configuration will be stored in a `~/.pyiceberg.yaml` file for CLI usage:

```yaml
catalog:
  rest:
    uri: http://localhost:8181
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: admin_secret
    s3.region: us-east-1
    s3.path-style-access: "true"
```

### 1.5 Default Table Properties

Every Iceberg table in this project will be created with the following baseline properties unless explicitly overridden:

```python
TABLE_DEFAULTS = {
    "format-version": "2",                       # Row-level deletes (equality + position)
    "write.format.default": "parquet",           # Columnar storage format
    "write.parquet.compression-codec": "zstd",   # Best compression-to-speed ratio
    "write.parquet.dict-encoding.enabled": "true",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
    "commit.retry.num-retries": "4",
    "commit.retry.min-wait-ms": "100",
    "read.split.target-size": "134217728",       # 128 MB target split size
}
```

**Why Format Version 2?**

- Enables row-level deletes via equality delete files and position delete files.
- Required for merge-on-read mode used heavily for SCD Type 2 expire-and-insert operations.
- Supports row-level updates without full partition rewrites.

**Why zstd compression?**

- Provides 20-30% better compression ratios than Snappy for columnar financial data.
- Decompression speed is within 10% of Snappy while compression is significantly better.
- Ideal for read-heavy analytical workloads on trade data.

---

## 2. Data Warehouse Architecture (Medallion / Lakehouse)

The data warehouse follows a three-tier Medallion architecture. Each tier is implemented as an Iceberg namespace (schema) with distinct purposes, data quality guarantees, and retention policies.

```
                    +-------------------------------------------------+
                    |              MinIO (S3 Object Storage)           |
                    |   s3://warehouse/                                |
                    |                                                  |
  Kafka Topics      |   +----------+   +-----------+   +-----------+  |   Downstream
  ================> |   | BRONZE   |-->| SILVER    |-->| GOLD      |  | =============>
  raw_trades        |   | Raw      |   | Cleaned & |   | Business  |  |   Dashboards
  raw_orderbook     |   | Events   |   | Enriched  |   | Ready     |  |   APIs
  raw_marketdata    |   +----------+   +-----------+   +-----------+  |   Analytics
                    |                                                  |
                    +-------------------------------------------------+
```

### 2.1 Bronze Layer (Raw)

**Purpose:** Landing zone for raw events directly from Kafka. Data is stored as-is with minimal transformation. Acts as the system of record and enables full reprocessing.

**Design Principles:**
- Schema-on-read: store raw payloads; validate only at the Silver layer.
- Append-only: never update or delete Bronze records (immutable audit trail).
- Include metadata columns: `_ingested_at`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`.

#### Bronze Tables

##### `bronze.raw_trades`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `trade_id`          | `string`    | Trade identifier from source               |
| `symbol`            | `string`    | Ticker symbol (e.g., AAPL)                 |
| `price`             | `double`    | Execution price                            |
| `quantity`          | `long`      | Number of shares / units                   |
| `trade_type`        | `string`    | BUY / SELL                                 |
| `order_type`        | `string`    | MARKET / LIMIT / STOP                      |
| `trader_id`         | `string`    | Trader identifier                          |
| `exchange`          | `string`    | Exchange code                              |
| `timestamp`         | `timestamp` | Event timestamp from source                |
| `raw_payload`       | `string`    | Full JSON payload for debugging            |
| `_ingested_at`      | `timestamp` | When the record landed in Bronze           |
| `_kafka_topic`      | `string`    | Source Kafka topic                          |
| `_kafka_partition`  | `int`       | Kafka partition                            |
| `_kafka_offset`     | `long`      | Kafka offset (for exactly-once tracking)   |

- **Partitioning:** `days(timestamp)` -- one partition per calendar day.
- **Sort order:** `symbol`, `timestamp`.
- **Retention:** 90 days (enforced via scheduled expiration job).

##### `bronze.raw_orderbook`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `symbol`            | `string`    | Ticker symbol                              |
| `snapshot_time`     | `timestamp` | Snapshot capture time                      |
| `bids`              | `string`    | JSON array of [price, qty] bid levels      |
| `asks`              | `string`    | JSON array of [price, qty] ask levels      |
| `exchange`          | `string`    | Exchange code                              |
| `raw_payload`       | `string`    | Full JSON payload                          |
| `_ingested_at`      | `timestamp` | Ingestion timestamp                        |
| `_kafka_topic`      | `string`    | Source Kafka topic                          |
| `_kafka_partition`  | `int`       | Kafka partition                            |
| `_kafka_offset`     | `long`      | Kafka offset                               |

- **Partitioning:** `days(snapshot_time)`.
- **Retention:** 90 days.

##### `bronze.raw_marketdata`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `symbol`            | `string`    | Ticker symbol                              |
| `tick_time`         | `timestamp` | Market data tick timestamp                 |
| `open`              | `double`    | Open price                                 |
| `high`              | `double`    | High price                                 |
| `low`               | `double`    | Low price                                  |
| `close`             | `double`    | Close price                                |
| `volume`            | `long`      | Volume                                     |
| `vwap`              | `double`    | Volume-weighted average price              |
| `interval`          | `string`    | Tick interval (1m, 5m, 1h, 1d)            |
| `exchange`          | `string`    | Exchange code                              |
| `raw_payload`       | `string`    | Full JSON payload                          |
| `_ingested_at`      | `timestamp` | Ingestion timestamp                        |
| `_kafka_topic`      | `string`    | Source Kafka topic                          |
| `_kafka_partition`  | `int`       | Kafka partition                            |
| `_kafka_offset`     | `long`      | Kafka offset                               |

- **Partitioning:** `days(tick_time)`.
- **Retention:** 90 days.

---

### 2.2 Silver Layer (Cleaned & Enriched)

**Purpose:** Conformed, deduplicated, type-cast data. This is the single source of truth for all downstream consumers. Reference data (symbols, exchanges) is joined here.

**Design Principles:**
- Deduplication by natural key + event timestamp.
- Null handling: fill with defaults or reject to a dead-letter table (`silver._rejected_records`).
- Type casting: all columns strongly typed (no raw strings for numeric fields).
- Enrichment: join with dimension seed data (symbol metadata, exchange info).
- Idempotent writes: use Iceberg's MERGE INTO semantics.

#### Silver Tables

##### `silver.trades`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `trade_id`          | `string`    | Deduplicated trade identifier              |
| `symbol`            | `string`    | Ticker symbol                              |
| `price`             | `decimal(18,6)` | Execution price (precise)              |
| `quantity`          | `long`      | Shares / units                             |
| `total_value`       | `decimal(18,6)` | `price * quantity`                     |
| `trade_type`        | `string`    | BUY / SELL (validated enum)                |
| `order_type`        | `string`    | MARKET / LIMIT / STOP (validated)          |
| `trader_id`         | `string`    | Trader identifier                          |
| `exchange`          | `string`    | Exchange code                              |
| `trade_timestamp`   | `timestamp` | Event timestamp (UTC normalized)           |
| `trade_date`        | `date`      | Derived date for partitioning              |
| `_source_offset`    | `long`      | Kafka offset for lineage                   |
| `_processed_at`     | `timestamp` | Silver processing timestamp                |

- **Partitioning:** `days(trade_timestamp)`, `bucket(16, symbol)`.
- **Deduplication key:** `trade_id`.

##### `silver.orderbook_snapshots`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `snapshot_id`       | `string`    | Generated surrogate (symbol + time hash)   |
| `symbol`            | `string`    | Ticker symbol                              |
| `snapshot_time`     | `timestamp` | Snapshot timestamp (UTC)                   |
| `best_bid_price`    | `decimal(18,6)` | Top-of-book bid                        |
| `best_bid_qty`      | `long`      | Top bid quantity                           |
| `best_ask_price`    | `decimal(18,6)` | Top-of-book ask                        |
| `best_ask_qty`      | `long`      | Top ask quantity                           |
| `spread`            | `decimal(18,6)` | `best_ask_price - best_bid_price`      |
| `bid_depth_5`       | `long`      | Total bid quantity across top 5 levels     |
| `ask_depth_5`       | `long`      | Total ask quantity across top 5 levels     |
| `exchange`          | `string`    | Exchange code                              |
| `_processed_at`     | `timestamp` | Silver processing timestamp                |

- **Partitioning:** `days(snapshot_time)`, `bucket(16, symbol)`.
- **Deduplication key:** `symbol` + `snapshot_time` + `exchange`.

##### `silver.market_data`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `market_data_id`    | `string`    | Generated surrogate                        |
| `symbol`            | `string`    | Ticker symbol                              |
| `tick_time`         | `timestamp` | Tick timestamp (UTC)                       |
| `interval`          | `string`    | Aggregation interval                       |
| `open`              | `decimal(18,6)` | Open price                             |
| `high`              | `decimal(18,6)` | High price                             |
| `low`               | `decimal(18,6)` | Low price                              |
| `close`             | `decimal(18,6)` | Close price                            |
| `volume`            | `long`      | Volume                                     |
| `vwap`              | `decimal(18,6)` | Volume-weighted average price          |
| `trade_count`       | `long`      | Number of trades in interval               |
| `exchange`          | `string`    | Exchange code                              |
| `_processed_at`     | `timestamp` | Silver processing timestamp                |

- **Partitioning:** `days(tick_time)`, `bucket(16, symbol)`.
- **Deduplication key:** `symbol` + `tick_time` + `interval` + `exchange`.

##### `silver.trader_activity`

| Column              | Type        | Description                                |
|---------------------|-------------|--------------------------------------------|
| `activity_id`       | `string`    | Generated surrogate                        |
| `trader_id`         | `string`    | Trader identifier                          |
| `activity_date`     | `date`      | Activity date                              |
| `symbol`            | `string`    | Ticker symbol                              |
| `total_buys`        | `long`      | Number of buy trades                       |
| `total_sells`       | `long`      | Number of sell trades                      |
| `buy_volume`        | `long`      | Total buy quantity                         |
| `sell_volume`       | `long`      | Total sell quantity                        |
| `buy_value`         | `decimal(18,6)` | Total buy notional value               |
| `sell_value`        | `decimal(18,6)` | Total sell notional value              |
| `net_position`      | `long`      | `buy_volume - sell_volume`                 |
| `avg_trade_size`    | `decimal(18,6)` | Average trade quantity                 |
| `_processed_at`     | `timestamp` | Silver processing timestamp                |

- **Partitioning:** `days(activity_date)`, `bucket(16, symbol)`.
- **Grain:** One row per trader per symbol per day.

---

### 2.3 Gold Layer (Business-Ready / Aggregated)

**Purpose:** Pre-computed metrics, KPIs, and denormalized views optimized for dashboard and API consumption. No further transformation should be needed by consumers.

**Design Principles:**
- Pre-aggregated at business-meaningful grains.
- Denormalized for fast reads (no joins required by consumers).
- Refresh cadence documented per table (some near-real-time, some daily batch).

#### Gold Tables

##### `gold.daily_trading_summary`

| Column                  | Type            | Description                            |
|-------------------------|-----------------|----------------------------------------|
| `summary_date`          | `date`          | Trading date                           |
| `symbol`                | `string`        | Ticker symbol                          |
| `company_name`          | `string`        | Denormalized from dim_symbol           |
| `sector`                | `string`        | Denormalized from dim_symbol           |
| `exchange`              | `string`        | Exchange code                          |
| `open_price`            | `decimal(18,6)` | First trade price of the day           |
| `close_price`           | `decimal(18,6)` | Last trade price of the day            |
| `high_price`            | `decimal(18,6)` | Day high                               |
| `low_price`             | `decimal(18,6)` | Day low                                |
| `vwap`                  | `decimal(18,6)` | Volume-weighted average price          |
| `total_volume`          | `long`          | Total shares traded                    |
| `total_value`           | `decimal(18,6)` | Total notional value                   |
| `trade_count`           | `long`          | Number of trades                       |
| `buy_volume`            | `long`          | Total buy volume                       |
| `sell_volume`           | `long`          | Total sell volume                      |
| `avg_spread`            | `decimal(18,6)` | Average bid-ask spread                 |
| `price_change_pct`      | `decimal(10,4)` | Daily price change percentage          |
| `volatility`            | `decimal(10,6)` | Intraday volatility (std dev returns)  |
| `_computed_at`          | `timestamp`     | Metric computation timestamp           |

- **Partitioning:** `days(summary_date)`.
- **Refresh cadence:** End-of-day batch + intraday incremental (every 15 min).
- **Grain:** One row per symbol per day.

##### `gold.trader_performance`

| Column                  | Type            | Description                            |
|-------------------------|-----------------|----------------------------------------|
| `performance_date`      | `date`          | Evaluation date                        |
| `trader_id`             | `string`        | Trader identifier                      |
| `trader_name`           | `string`        | Denormalized from dim_trader           |
| `account_type`          | `string`        | Denormalized from dim_trader           |
| `risk_category`         | `string`        | Denormalized from dim_trader           |
| `total_trades`          | `long`          | Trade count for the day                |
| `total_volume`          | `long`          | Total shares traded                    |
| `total_notional`        | `decimal(18,6)` | Total trade value                      |
| `realized_pnl`          | `decimal(18,6)` | Realized profit/loss                   |
| `win_rate`              | `decimal(5,4)`  | Fraction of profitable trades          |
| `avg_hold_time_sec`     | `long`          | Average holding period in seconds      |
| `max_drawdown_pct`      | `decimal(10,4)` | Max intraday drawdown percentage       |
| `sharpe_ratio`          | `decimal(10,6)` | Risk-adjusted return metric            |
| `distinct_symbols`      | `int`           | Number of unique symbols traded        |
| `_computed_at`          | `timestamp`     | Metric computation timestamp           |

- **Partitioning:** `days(performance_date)`.
- **Grain:** One row per trader per day.

##### `gold.market_overview`

| Column                  | Type            | Description                            |
|-------------------------|-----------------|----------------------------------------|
| `overview_date`         | `date`          | Market date                            |
| `exchange`              | `string`        | Exchange code                          |
| `total_trades`          | `long`          | Exchange-wide trade count              |
| `total_volume`          | `long`          | Exchange-wide volume                   |
| `total_value`           | `decimal(18,6)` | Exchange-wide notional value           |
| `active_symbols`        | `int`           | Symbols with at least one trade        |
| `active_traders`        | `int`           | Traders with at least one trade        |
| `advancers`             | `int`           | Symbols with positive price change     |
| `decliners`             | `int`           | Symbols with negative price change     |
| `unchanged`             | `int`           | Symbols with no price change           |
| `avg_spread`            | `decimal(18,6)` | Market-wide average spread             |
| `market_volatility_idx` | `decimal(10,6)` | Composite volatility index             |
| `_computed_at`          | `timestamp`     | Metric computation timestamp           |

- **Partitioning:** `days(overview_date)`.
- **Grain:** One row per exchange per day.

##### `gold.portfolio_positions`

| Column                  | Type            | Description                            |
|-------------------------|-----------------|----------------------------------------|
| `position_date`         | `date`          | Position as-of date                    |
| `trader_id`             | `string`        | Trader identifier                      |
| `account_id`            | `string`        | Trading account                        |
| `symbol`                | `string`        | Ticker symbol                          |
| `quantity`              | `long`          | Net position (positive=long, neg=short)|
| `avg_cost_basis`        | `decimal(18,6)` | Weighted average entry price           |
| `current_price`         | `decimal(18,6)` | Last traded price                      |
| `market_value`          | `decimal(18,6)` | `quantity * current_price`             |
| `unrealized_pnl`        | `decimal(18,6)` | Unrealized profit/loss                 |
| `unrealized_pnl_pct`    | `decimal(10,4)` | P&L as percentage                      |
| `weight_pct`            | `decimal(10,4)` | Position weight in portfolio           |
| `_computed_at`          | `timestamp`     | Computation timestamp                  |

- **Partitioning:** `days(position_date)`.
- **Grain:** One row per trader per account per symbol per day.

##### `gold.trade_analytics`

| Column                  | Type            | Description                            |
|-------------------------|-----------------|----------------------------------------|
| `analytics_date`        | `date`          | Analysis date                          |
| `symbol`                | `string`        | Ticker symbol                          |
| `sector`                | `string`        | Sector from dim_symbol                 |
| `industry`              | `string`        | Industry from dim_symbol               |
| `exchange`              | `string`        | Exchange code                          |
| `market_session`        | `string`        | PRE_MARKET / REGULAR / AFTER_HOURS     |
| `trade_count`           | `long`          | Number of trades                       |
| `volume`                | `long`          | Total volume                           |
| `notional_value`        | `decimal(18,6)` | Total notional value                   |
| `avg_trade_size`        | `decimal(18,6)` | Average quantity per trade             |
| `buy_sell_ratio`        | `decimal(10,4)` | Buy volume / sell volume               |
| `large_trade_count`     | `long`          | Trades above threshold size            |
| `large_trade_pct`       | `decimal(10,4)` | Fraction of large trades by volume     |
| `price_impact_avg`      | `decimal(10,6)` | Avg price impact per trade             |
| `_computed_at`          | `timestamp`     | Computation timestamp                  |

- **Partitioning:** `days(analytics_date)`.
- **Grain:** One row per symbol per exchange per market session per day.

---

## 3. Dimensional Data Model

### 3.1 Entity-Relationship Overview

```
                          +----------------+
                          |   dim_time     |
                          | (SCD Type 1)   |
                          +-------+--------+
                                  |
                                  | time_key
                                  |
+----------------+       +--------+--------+       +----------------+
|  dim_symbol    |       |                 |       |  dim_exchange  |
|  (SCD Type 2)  +-------+  fact_trades    +-------+  (SCD Type 1)  |
+----------------+  sym  |                 |  exch +----------------+
                   _key  +--------+--------+  _key
                                  |
                                  | trader_key
                                  |
                          +-------+--------+
                          |  dim_trader    |
                          |  (SCD Type 2)  |
                          +----------------+

                          +----------------+
                          |  dim_account   |
                          |  (SCD Type 3)  |
                          +----------------+
                          (linked via trader_id)


+----------------+       +--------+--------+
|  dim_symbol    |       |                 |
|  (SCD Type 2)  +-------+ fact_orderbook  |
+----------------+  sym  |                 |
                   _key  +--------+--------+
                                  |
                                  | time_key
                                  |
                          +-------+--------+
                          |   dim_time     |
                          +----------------+
```

### 3.2 Fact Tables

#### `fact_trades`

**Grain:** One row per trade execution.

| Column          | Type            | Key Type    | Description                          |
|-----------------|-----------------|-------------|--------------------------------------|
| `trade_id`      | `string`        | Natural PK  | Unique trade identifier              |
| `symbol_key`    | `long`          | FK          | Surrogate key to `dim_symbol`        |
| `trader_key`    | `long`          | FK          | Surrogate key to `dim_trader`        |
| `time_key`      | `long`          | FK          | Surrogate key to `dim_time`          |
| `exchange_key`  | `long`          | FK          | Surrogate key to `dim_exchange`      |
| `price`         | `decimal(18,6)` | Measure     | Execution price                      |
| `quantity`      | `long`          | Measure     | Trade quantity                       |
| `total_value`   | `decimal(18,6)` | Measure     | `price * quantity`                   |
| `commission`    | `decimal(18,6)` | Measure     | Broker commission                    |
| `trade_type`    | `string`        | Degenerate  | BUY / SELL                           |
| `order_type`    | `string`        | Degenerate  | MARKET / LIMIT / STOP                |
| `status`        | `string`        | Degenerate  | EXECUTED / PARTIAL / CANCELLED       |

- **Partitioning:** `days(event_timestamp)` via a hidden partition column derived from `time_key` lookup.
- **Sort order:** `symbol_key`, `time_key`.
- **Expected grain volume:** ~50,000-500,000 rows per day depending on market activity.

#### `fact_orderbook`

**Grain:** One row per symbol per snapshot interval.

| Column          | Type            | Key Type    | Description                          |
|-----------------|-----------------|-------------|--------------------------------------|
| `snapshot_id`   | `string`        | Natural PK  | Unique snapshot identifier           |
| `symbol_key`    | `long`          | FK          | Surrogate key to `dim_symbol`        |
| `time_key`      | `long`          | FK          | Surrogate key to `dim_time`          |
| `bid_price`     | `decimal(18,6)` | Measure     | Best bid price                       |
| `ask_price`     | `decimal(18,6)` | Measure     | Best ask price                       |
| `spread`        | `decimal(18,6)` | Measure     | `ask_price - bid_price`              |
| `bid_depth`     | `long`          | Measure     | Total bid quantity (top 5 levels)    |
| `ask_depth`     | `long`          | Measure     | Total ask quantity (top 5 levels)    |
| `mid_price`     | `decimal(18,6)` | Measure     | `(bid_price + ask_price) / 2`        |

- **Partitioning:** `days(snapshot_timestamp)`, `bucket(16, symbol_key)`.
- **Sort order:** `symbol_key`, `time_key`.

---

### 3.3 Dimension Tables

#### `dim_symbol` (SCD Type 2 -- Historical Tracking)

| Column                | Type        | Description                                |
|-----------------------|-------------|--------------------------------------------|
| `symbol_key`          | `long`      | Surrogate key (auto-generated)             |
| `symbol`              | `string`    | Natural key (e.g., AAPL)                   |
| `company_name`        | `string`    | Full company name                          |
| `sector`              | `string`    | GICS sector                                |
| `industry`            | `string`    | GICS industry                              |
| `market_cap_category` | `string`    | MEGA / LARGE / MID / SMALL / MICRO         |
| `listing_exchange`    | `string`    | Primary listing exchange                   |
| `ipo_date`            | `date`      | IPO date                                   |
| `effective_date`      | `date`      | SCD2: row validity start                   |
| `expiry_date`         | `date`      | SCD2: row validity end (9999-12-31 if current) |
| `is_current`          | `boolean`   | SCD2: true if this is the active version   |
| `_row_hash`           | `string`    | MD5 hash of tracked columns for change detection |

#### `dim_trader` (SCD Type 2 -- Historical Tracking)

| Column                | Type        | Description                                |
|-----------------------|-------------|--------------------------------------------|
| `trader_key`          | `long`      | Surrogate key (auto-generated)             |
| `trader_id`           | `string`    | Natural key                                |
| `name`                | `string`    | Trader full name                           |
| `account_type`        | `string`    | INDIVIDUAL / INSTITUTIONAL / ALGORITHMIC   |
| `risk_category`       | `string`    | LOW / MEDIUM / HIGH                        |
| `trading_tier`        | `string`    | BRONZE / SILVER / GOLD / PLATINUM          |
| `registration_date`   | `date`      | Account creation date                      |
| `effective_date`      | `date`      | SCD2: row validity start                   |
| `expiry_date`         | `date`      | SCD2: row validity end                     |
| `is_current`          | `boolean`   | SCD2: active version flag                  |
| `_row_hash`           | `string`    | MD5 hash of tracked columns                |

#### `dim_exchange` (SCD Type 1 -- Overwrite)

| Column                | Type        | Description                                |
|-----------------------|-------------|--------------------------------------------|
| `exchange_key`        | `long`      | Surrogate key                              |
| `exchange_code`       | `string`    | Natural key (e.g., NYSE, NASDAQ)           |
| `exchange_name`       | `string`    | Full name                                  |
| `timezone`            | `string`    | Timezone (e.g., America/New_York)          |
| `country`             | `string`    | Country code (US, UK, JP)                  |
| `currency`            | `string`    | Primary trading currency                   |
| `market_open_time`    | `string`    | Market open (HH:MM)                        |
| `market_close_time`   | `string`    | Market close (HH:MM)                       |
| `_updated_at`         | `timestamp` | Last update timestamp                      |

#### `dim_time` (SCD Type 1 -- Pre-populated)

| Column                | Type        | Description                                |
|-----------------------|-------------|--------------------------------------------|
| `time_key`            | `long`      | Surrogate key (YYYYMMDDHHMI format)        |
| `full_timestamp`      | `timestamp` | Full datetime                              |
| `date`                | `date`      | Calendar date                              |
| `year`                | `int`       | Year (2024, 2025, ...)                     |
| `quarter`             | `int`       | Quarter (1-4)                              |
| `month`               | `int`       | Month (1-12)                               |
| `month_name`          | `string`    | Month name (January, ...)                  |
| `week`                | `int`       | ISO week number                            |
| `day_of_month`        | `int`       | Day of month (1-31)                        |
| `day_of_week`         | `int`       | Day of week (1=Monday, 7=Sunday)           |
| `day_name`            | `string`    | Day name (Monday, ...)                     |
| `is_weekend`          | `boolean`   | Saturday or Sunday                         |
| `is_trading_day`      | `boolean`   | Market open (excludes holidays/weekends)   |
| `market_session`      | `string`    | PRE_MARKET / REGULAR / AFTER_HOURS / CLOSED|
| `hour`                | `int`       | Hour (0-23)                                |
| `minute`              | `int`       | Minute (0-59)                              |

- **Grain:** One row per minute. Pre-generated for 2 years (525,600 rows per year).
- **Key format:** `time_key` = `YYYYMMDDHHMI` as a long integer (e.g., `202501151430` = Jan 15, 2025 at 14:30).

#### `dim_account` (SCD Type 3 -- Previous Value)

| Column                  | Type        | Description                              |
|-------------------------|-------------|------------------------------------------|
| `account_key`           | `long`      | Surrogate key                            |
| `account_id`            | `string`    | Natural key                              |
| `trader_id`             | `string`    | Owner trader ID                          |
| `current_tier`          | `string`    | Current account tier                     |
| `previous_tier`         | `string`    | Previous tier (NULL if never changed)    |
| `tier_change_date`      | `date`      | When tier last changed                   |
| `current_status`        | `string`    | Current status (ACTIVE/SUSPENDED/CLOSED) |
| `previous_status`       | `string`    | Previous status (NULL if never changed)  |
| `status_change_date`    | `date`      | When status last changed                 |
| `account_open_date`     | `date`      | Account creation date                    |
| `account_type`          | `string`    | MARGIN / CASH / IRA                      |
| `_updated_at`           | `timestamp` | Last update timestamp                    |

---

## 4. SCD Implementation Details

### 4.1 SCD Type 1 (Overwrite) -- `dim_exchange`, `dim_time`

**Behavior:** When a change is detected, the existing record is updated in place. No history is retained.

**Implementation approach:**

```python
# File: libs/data-models/scd/scd_type1.py

from pyiceberg.table import Table
from pyiceberg.expressions import EqualTo


def apply_scd_type1(table: Table, incoming_df, natural_key: str):
    """
    SCD Type 1: Overwrite existing records with incoming values.

    Steps:
    1. Read current records from the Iceberg table.
    2. Identify records that exist in both current and incoming (by natural key).
    3. For matched records, compare all non-key columns.
    4. If differences found, perform an overwrite merge:
       a. Delete the old record (Iceberg equality delete).
       b. Insert the updated record.
    5. For new records (in incoming but not current), append directly.
    """
    # Pseudocode for the merge logic:
    #
    # current_df = table.scan().to_pandas()
    # merged = incoming_df.merge(current_df, on=natural_key, how='left', indicator=True)
    #
    # new_records = merged[merged['_merge'] == 'left_only']
    # matched = merged[merged['_merge'] == 'both']
    # changed = matched[matched columns differ]
    #
    # Use Iceberg overwrite to replace changed partitions
    # Append new_records
    pass
```

**Use cases and rationale:**
- `dim_exchange`: Exchange attributes (timezone, open/close hours) change rarely. When they do, historical queries do not need the old value -- the current state is always correct.
- `dim_time`: Pre-populated and static. Updates only happen when trading calendars are refreshed (e.g., new holidays added for the upcoming year).

---

### 4.2 SCD Type 2 (Historical Tracking) -- `dim_symbol`, `dim_trader`

**Behavior:** When a change is detected, the current record is expired and a new record is inserted. This preserves full history so that fact table foreign keys always point to the dimension version that was active at transaction time.

**Surrogate Key Generation Strategy:**

```
surrogate_key = global_sequence_generator()
```

Use an auto-incrementing sequence stored in a control table (`_metadata.surrogate_keys`) to guarantee uniqueness:

| Table Name   | Last Issued Key |
|--------------|-----------------|
| dim_symbol   | 100042          |
| dim_trader   | 200085          |

**Change Detection via Hash Comparison:**

Tracked columns are hashed using MD5 to create a `_row_hash`. On each load, the incoming hash is compared against the current hash to detect changes without comparing every column individually.

```python
import hashlib

def compute_row_hash(row: dict, tracked_columns: list[str]) -> str:
    """Compute MD5 hash of tracked column values for change detection."""
    values = "|".join(str(row.get(col, "")) for col in sorted(tracked_columns))
    return hashlib.md5(values.encode()).hexdigest()
```

**Tracked columns per dimension:**

| Dimension    | Tracked Columns (changes trigger new version)                                |
|--------------|-------------------------------------------------------------------------------|
| dim_symbol   | `company_name`, `sector`, `industry`, `market_cap_category`, `listing_exchange` |
| dim_trader   | `name`, `account_type`, `risk_category`, `trading_tier`                       |

**Expire-and-Insert Logic:**

```python
# File: libs/data-models/scd/scd_type2.py

from datetime import date, datetime
from pyiceberg.table import Table


def apply_scd_type2(
    table: Table,
    incoming_df,
    natural_key: str,
    tracked_columns: list[str],
    surrogate_key_col: str,
):
    """
    SCD Type 2: Expire old records and insert new versions.

    Steps:
    1. Read current active records (is_current = true).
    2. Compute _row_hash for incoming records.
    3. Join incoming with current on natural_key.
    4. Identify CHANGED records (hash mismatch).
    5. Identify NEW records (no match in current).
    6. For CHANGED records:
       a. Expire: UPDATE SET expiry_date = today - 1, is_current = false
          (implemented via Iceberg row-level delete + re-insert of expired version)
       b. Insert: New row with new surrogate_key, effective_date = today,
          expiry_date = '9999-12-31', is_current = true
    7. For NEW records:
       a. Insert with new surrogate_key, effective_date = today,
          expiry_date = '9999-12-31', is_current = true
    """
    FUTURE_DATE = date(9999, 12, 31)
    today = date.today()

    # --- Step 1: Read current active records ---
    # current_df = table.scan(
    #     row_filter=EqualTo("is_current", True)
    # ).to_pandas()

    # --- Step 2: Compute hashes ---
    # incoming_df["_row_hash"] = incoming_df.apply(
    #     lambda row: compute_row_hash(row, tracked_columns), axis=1
    # )

    # --- Step 3-5: Detect changes ---
    # merged = incoming_df.merge(current_df, on=natural_key, suffixes=("_new", "_cur"))
    # changed = merged[merged["_row_hash_new"] != merged["_row_hash_cur"]]
    # new_records = incoming_df[~incoming_df[natural_key].isin(current_df[natural_key])]

    # --- Step 6: Expire old + insert new version ---
    # For each changed record:
    #   - Create expired copy: set expiry_date=today-1, is_current=False
    #   - Create new version: new surrogate key, effective_date=today,
    #     expiry_date=FUTURE_DATE, is_current=True
    #
    # Use Iceberg merge-on-read:
    #   table.delete(EqualTo(natural_key, value) & EqualTo("is_current", True))
    #   table.append(expired_records_df)  # historical snapshot
    #   table.append(new_version_df)      # current version

    # --- Step 7: Insert brand new records ---
    # table.append(new_records_with_scd_columns)
    pass
```

**Iceberg Merge-on-Read for Efficient Updates:**

Format version 2 enables row-level deletes via equality delete files. When expiring a record:
1. An equality delete file is written targeting the old row (matched by `natural_key` + `is_current = true`).
2. The expired version (with updated `expiry_date` and `is_current = false`) is appended.
3. The new current version is appended.
4. On read, Iceberg merges these files transparently.
5. Periodic compaction collapses delete files into rewritten data files for read performance.

---

### 4.3 SCD Type 3 (Previous Value) -- `dim_account`

**Behavior:** Stores the current value and one previous value for tracked attributes. Provides limited historical context without the complexity of SCD Type 2.

**Implementation:**

```python
# File: libs/data-models/scd/scd_type3.py

from datetime import date
from pyiceberg.table import Table


def apply_scd_type3(
    table: Table,
    incoming_df,
    natural_key: str,
    tracked_pairs: list[tuple[str, str, str]],
    # tracked_pairs format: [(current_col, previous_col, change_date_col), ...]
):
    """
    SCD Type 3: Update current value, shift old value to previous column.

    tracked_pairs example:
        [
            ("current_tier", "previous_tier", "tier_change_date"),
            ("current_status", "previous_status", "status_change_date"),
        ]

    Steps:
    1. Read current records from Iceberg table.
    2. Join incoming with current on natural_key.
    3. For each tracked pair (current_X, previous_X, change_date_X):
       a. If incoming.current_X != existing.current_X:
          - Set previous_X = existing.current_X
          - Set current_X  = incoming.current_X
          - Set change_date_X = today
       b. Otherwise: no change.
    4. Overwrite the record in Iceberg (row-level delete + insert).
    """
    today = date.today()

    # For each matched record with detected changes:
    #   for current_col, previous_col, change_date_col in tracked_pairs:
    #       if incoming[current_col] != existing[current_col]:
    #           updated[previous_col] = existing[current_col]
    #           updated[current_col] = incoming[current_col]
    #           updated[change_date_col] = today
    #
    # Use Iceberg row-level delete + append to replace the record.
    pass
```

**SCD Type 3 column mapping for `dim_account`:**

| Tracked Attribute | Current Column    | Previous Column    | Change Date Column   |
|-------------------|-------------------|--------------------|----------------------|
| Tier              | `current_tier`    | `previous_tier`    | `tier_change_date`   |
| Status            | `current_status`  | `previous_status`  | `status_change_date` |

---

## 5. Partitioning Strategy

### 5.1 Iceberg Hidden Partitioning

Iceberg uses partition transforms applied to source columns, eliminating the need for users to include partition columns in queries. The query engine automatically applies partition pruning from predicates on the source columns.

### 5.2 Partition Specifications by Table

| Table                         | Partition Spec                                  | Rationale                                          |
|-------------------------------|------------------------------------------------|----------------------------------------------------|
| `bronze.raw_trades`           | `days(timestamp)`                              | Daily ingestion; time-range queries                |
| `bronze.raw_orderbook`        | `days(snapshot_time)`                          | Daily ingestion; time-range queries                |
| `bronze.raw_marketdata`       | `days(tick_time)`                              | Daily ingestion; time-range queries                |
| `silver.trades`               | `days(trade_timestamp), bucket(16, symbol)`    | Time + symbol queries; 16 buckets for even spread  |
| `silver.orderbook_snapshots`  | `days(snapshot_time), bucket(16, symbol)`      | Time + symbol queries                              |
| `silver.market_data`          | `days(tick_time), bucket(16, symbol)`          | Time + symbol queries                              |
| `silver.trader_activity`      | `days(activity_date), bucket(16, symbol)`      | Time + symbol queries                              |
| `fact_trades`                 | `days(event_timestamp)`                        | Time-range analysis                                |
| `fact_orderbook`              | `days(snapshot_timestamp), bucket(16, symbol_key)` | Time + symbol lookup                           |
| `gold.daily_trading_summary`  | `days(summary_date)`                           | Daily grain; date-range dashboards                 |
| `gold.trader_performance`     | `days(performance_date)`                       | Daily grain                                        |
| `gold.market_overview`        | `days(overview_date)`                          | Daily grain                                        |
| `gold.portfolio_positions`    | `days(position_date)`                          | Daily grain                                        |
| `gold.trade_analytics`        | `days(analytics_date)`                         | Daily grain                                        |
| `dim_symbol`                  | `bucket(8, symbol)`                            | Lookup by symbol; small table                      |
| `dim_trader`                  | `bucket(8, trader_id)`                         | Lookup by trader_id; small table                   |
| `dim_exchange`                | None (unpartitioned)                           | Very small table (<20 rows)                        |
| `dim_time`                    | `years(date)`                                  | Pre-populated by year; ~525K rows/year             |
| `dim_account`                 | `bucket(8, account_id)`                        | Lookup by account_id; small table                  |

### 5.3 Bucket Count Rationale

- **16 buckets for Silver/Fact tables:** Assuming ~100-500 unique symbols, 16 buckets provide roughly 6-30 symbols per bucket. This yields adequately sized Parquet files (target: 128-256 MB each) while enabling effective partition pruning.
- **8 buckets for Dimensions:** Dimension tables are small. 8 buckets prevent over-partitioning into tiny files.

### 5.4 Partition Evolution

Iceberg supports changing partition schemes without rewriting existing data:

```python
# Example: Adding symbol bucketing to an existing time-partitioned table
from pyiceberg.transforms import BucketTransform, DayTransform

table = catalog.load_table("silver.trades")

# Evolve the partition spec (existing data remains in old scheme)
with table.update_spec() as update:
    update.add_field(
        source_column_name="symbol",
        transform=BucketTransform(num_buckets=16),
        name="symbol_bucket",
    )

# New data writes use the evolved spec
# Old data is read correctly using the old spec metadata
# Optionally rewrite old data to match new spec for consistent performance:
# table.rewrite_data_files(...)
```

---

## 6. Table Maintenance

### 6.1 Compaction Strategy

Small files accumulate from streaming ingestion and SCD operations. Compaction rewrites them into optimally-sized files.

**File:** `services/maintenance/compaction.py`

```python
# Compaction configuration
COMPACTION_CONFIG = {
    "target-file-size-bytes": 256 * 1024 * 1024,  # 256 MB target per file
    "min-input-files": 5,                          # Minimum files to trigger compaction
    "max-concurrent-file-group-rewrites": 4,       # Parallelism
    "partial-progress.enabled": True,              # Commit incrementally
}

# Schedule:
# - Bronze tables: compact every 6 hours (high ingestion rate)
# - Silver tables: compact every 12 hours
# - Gold tables:   compact daily (batch writes, fewer small files)
# - Dimension tables: compact weekly (low change frequency)
```

**Compaction via PyIceberg:**

```python
def compact_table(table_name: str):
    """Rewrite data files to optimize file sizes and remove delete files."""
    table = catalog.load_table(table_name)
    table.rewrite_data_files(
        target_size_in_bytes=256 * 1024 * 1024,
        min_input_files=5,
    )
```

### 6.2 Snapshot Expiration

Iceberg retains snapshots for time travel. Expired snapshots must be cleaned up to free storage.

```python
def expire_snapshots(table_name: str, retain_days: int = 7):
    """Remove snapshots older than retention period."""
    from datetime import datetime, timedelta

    table = catalog.load_table(table_name)
    cutoff = datetime.now() - timedelta(days=retain_days)

    table.expire_snapshots(
        older_than=cutoff,
        retain_last=5,  # Always keep at least 5 snapshots
    )
```

**Retention policies:**

| Table Layer  | Snapshot Retention | Rationale                                       |
|--------------|-------------------|-------------------------------------------------|
| Bronze       | 7 days            | Reprocessing from Kafka offsets is the fallback  |
| Silver       | 14 days           | Allows 2-week time travel for debugging          |
| Gold         | 30 days           | Business users may need historical comparisons   |
| Dimensions   | 30 days           | SCD history is in the data; snapshots for rollback |

### 6.3 Orphan File Cleanup

Orphan files are data files not referenced by any snapshot (from failed writes or expired snapshots).

```python
def cleanup_orphan_files(table_name: str):
    """Remove data files not referenced by any metadata snapshot."""
    table = catalog.load_table(table_name)

    # List all files in the table location on MinIO
    # Compare against files referenced in current + retained snapshots
    # Delete unreferenced files older than 3 days (safety buffer)

    table.remove_orphan_files(
        older_than_ms=3 * 24 * 60 * 60 * 1000,  # 3 days in milliseconds
    )
```

### 6.4 Sort Order Optimization

Sorting data within files improves query performance by enabling min/max statistics-based pruning.

```python
def set_sort_order(table_name: str):
    """Configure sort order for optimal query patterns."""
    table = catalog.load_table(table_name)

    # Primary query patterns for trades: WHERE symbol = X AND timestamp BETWEEN ...
    with table.replace_sort_order() as update:
        update.asc("symbol")
        update.asc("timestamp")
```

**Sort orders by table:**

| Table                        | Sort Order                        |
|------------------------------|-----------------------------------|
| `bronze.raw_trades`          | `symbol ASC, timestamp ASC`       |
| `silver.trades`              | `symbol ASC, trade_timestamp ASC` |
| `fact_trades`                | `symbol_key ASC, time_key ASC`    |
| `fact_orderbook`             | `symbol_key ASC, time_key ASC`    |
| `gold.daily_trading_summary` | `symbol ASC, summary_date ASC`    |

### 6.5 Manifest Cleanup

Manifest files accumulate across snapshots. Clean up manifests no longer referenced by any live snapshot.

```python
def cleanup_manifests(table_name: str):
    """Remove manifest files not referenced by retained snapshots."""
    table = catalog.load_table(table_name)
    # This is typically handled as part of expire_snapshots + rewrite_manifests
    table.rewrite_manifests()
```

### 6.6 Maintenance Schedule (Dagster Integration)

All maintenance jobs will be orchestrated via Dagster schedules:

| Job                   | Schedule                | Tables                     |
|-----------------------|-------------------------|----------------------------|
| `compact_bronze`      | Every 6 hours           | All `bronze.*` tables      |
| `compact_silver`      | Every 12 hours          | All `silver.*` tables      |
| `compact_gold`        | Daily at 02:00 UTC      | All `gold.*` tables        |
| `compact_dimensions`  | Weekly (Sunday 03:00)   | All `dim_*` tables         |
| `expire_snapshots`    | Daily at 04:00 UTC      | All tables                 |
| `orphan_cleanup`      | Weekly (Sunday 05:00)   | All tables                 |
| `rewrite_manifests`   | Weekly (Sunday 06:00)   | All tables                 |

---

## 7. Schema Evolution

Iceberg provides robust schema evolution guarantees. Column changes are tracked by internal column IDs, not by name or position. This means renames, reorders, and type changes are safe and backward compatible.

### 7.1 Adding Columns (Backward Compatible)

Adding a new column does not break existing queries or data. Old data files return `NULL` for the new column.

```python
def add_column(table_name: str, column_name: str, column_type, doc: str = None):
    """Add a new column to an existing Iceberg table."""
    table = catalog.load_table(table_name)
    with table.update_schema() as update:
        update.add_column(column_name, column_type, doc=doc)

# Example: Add a 'settlement_date' column to fact_trades
# add_column("fact_trades", "settlement_date", DateType(), doc="T+2 settlement date")
```

### 7.2 Renaming Columns

Renames update metadata only. Data files are not rewritten. Queries using the old name will fail; queries using the new name work across all data files (old and new).

```python
def rename_column(table_name: str, old_name: str, new_name: str):
    """Rename a column (metadata-only operation)."""
    table = catalog.load_table(table_name)
    with table.update_schema() as update:
        update.rename_column(old_name, new_name)

# Example: Rename 'qty' to 'quantity'
# rename_column("silver.trades", "qty", "quantity")
```

### 7.3 Type Promotion

Iceberg allows safe type widening. Data files written with the narrower type are read correctly after promotion.

**Supported promotions:**

| From            | To              | Safe? |
|-----------------|-----------------|-------|
| `int`           | `long`          | Yes   |
| `float`         | `double`        | Yes   |
| `decimal(P, S)` | `decimal(P', S)` where P' > P | Yes |

```python
def promote_column_type(table_name: str, column_name: str, new_type):
    """Widen a column's type (e.g., int -> long)."""
    table = catalog.load_table(table_name)
    with table.update_schema() as update:
        update.update_column(column_name, new_type)

# Example: Promote quantity from int to long
# promote_column_type("silver.trades", "quantity", LongType())
```

### 7.4 Schema Evolution Guarantees

| Operation          | Data Rewrite Required? | Backward Compatible? | Notes                              |
|--------------------|------------------------|----------------------|------------------------------------|
| Add column         | No                     | Yes                  | Old files return NULL              |
| Drop column        | No                     | No (breaking)        | Old data still in files but hidden |
| Rename column      | No                     | No (queries by name) | Internal ID unchanged              |
| Reorder columns    | No                     | Yes                  | Columns read by ID, not position   |
| Type promotion     | No                     | Yes                  | Readers widen automatically        |
| Type narrowing     | N/A                    | N/A                  | Not supported (would lose data)    |

### 7.5 Schema Evolution Policy

To maintain consistency across the project:

1. **All schema changes must be version-controlled** in migration files under `libs/data-models/migrations/`.
2. **Migration file naming:** `V{version}__{description}.py` (e.g., `V003__add_settlement_date_to_fact_trades.py`).
3. **Breaking changes** (drop column, type narrowing) require a major version bump and coordinated deployment across all consumers.
4. **Non-breaking changes** (add column, type promotion, rename) can be applied independently.
5. **Dagster sensors** detect schema changes and trigger downstream pipeline re-validation.

---

## 8. Implementation Steps

### 8.1 File Structure

```
DE-project/
├── infra/
│   └── docker/
│       └── docker-compose.storage.yml          # MinIO + Iceberg REST catalog
│
├── libs/
│   └── data-models/
│       ├── __init__.py
│       ├── catalog.py                          # PyIceberg catalog connection
│       ├── table_properties.py                 # Default table properties
│       ├── schemas/
│       │   ├── __init__.py
│       │   ├── bronze.py                       # Bronze table schemas (PyIceberg Schema objects)
│       │   ├── silver.py                       # Silver table schemas
│       │   ├── gold.py                         # Gold table schemas
│       │   ├── facts.py                        # Fact table schemas
│       │   └── dimensions.py                   # Dimension table schemas
│       ├── ddl/
│       │   ├── 01_create_namespaces.sql        # CREATE NAMESPACE bronze, silver, gold, dim
│       │   ├── 02_bronze_tables.sql            # CREATE TABLE statements for Bronze
│       │   ├── 03_silver_tables.sql            # CREATE TABLE statements for Silver
│       │   ├── 04_gold_tables.sql              # CREATE TABLE statements for Gold
│       │   ├── 05_fact_tables.sql              # CREATE TABLE statements for Facts
│       │   └── 06_dimension_tables.sql         # CREATE TABLE statements for Dimensions
│       ├── scd/
│       │   ├── __init__.py
│       │   ├── scd_type1.py                    # SCD Type 1 implementation
│       │   ├── scd_type2.py                    # SCD Type 2 implementation
│       │   ├── scd_type3.py                    # SCD Type 3 implementation
│       │   └── surrogate_keys.py               # Surrogate key generation
│       ├── partitioning/
│       │   ├── __init__.py
│       │   └── partition_specs.py              # Partition spec definitions per table
│       ├── migrations/
│       │   ├── __init__.py
│       │   └── V001__initial_schema.py         # Initial schema creation migration
│       ├── seed/
│       │   ├── seed_dim_time.py                # Pre-populate dim_time
│       │   ├── seed_dim_exchange.py            # Seed exchange reference data
│       │   └── exchange_reference.csv          # Exchange reference data file
│       └── maintenance/
│           ├── __init__.py
│           ├── compaction.py                   # Compaction jobs
│           ├── snapshot_expiration.py          # Snapshot expiration jobs
│           ├── orphan_cleanup.py               # Orphan file cleanup
│           └── manifest_rewrite.py             # Manifest rewrite jobs
│
├── services/
│   └── storage/
│       ├── __init__.py
│       ├── bronze_writer.py                    # Kafka -> Bronze Iceberg writer
│       ├── silver_processor.py                 # Bronze -> Silver transformation logic
│       ├── gold_aggregator.py                  # Silver -> Gold aggregation logic
│       ├── dimension_loader.py                 # Dimension table load orchestrator
│       └── fact_loader.py                      # Fact table load (join Silver + Dimensions)
│
└── tests/
    └── data_models/
        ├── __init__.py
        ├── conftest.py                         # Shared fixtures (test catalog, sample data)
        ├── test_schemas.py                     # Schema validation tests
        ├── test_scd_type1.py                   # SCD Type 1 logic tests
        ├── test_scd_type2.py                   # SCD Type 2 logic tests
        ├── test_scd_type3.py                   # SCD Type 3 logic tests
        ├── test_partitioning.py                # Partition spec validation
        ├── test_data_integrity.py              # Referential integrity tests
        └── test_maintenance.py                 # Maintenance job tests
```

### 8.2 Implementation Phases

#### Phase 1: Infrastructure (Week 1)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 1.1  | Create Docker Compose for MinIO + Iceberg REST | `infra/docker/docker-compose.storage.yml`           |
| 1.2  | Validate MinIO bucket creation                 | Manual / integration test                           |
| 1.3  | Validate Iceberg REST catalog connectivity     | `libs/data-models/catalog.py`                       |
| 1.4  | Define default table properties                | `libs/data-models/table_properties.py`              |

#### Phase 2: Schema Definitions (Week 1-2)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 2.1  | Define Bronze table schemas in Python          | `libs/data-models/schemas/bronze.py`                |
| 2.2  | Define Silver table schemas in Python          | `libs/data-models/schemas/silver.py`                |
| 2.3  | Define Gold table schemas in Python            | `libs/data-models/schemas/gold.py`                  |
| 2.4  | Define Fact table schemas in Python            | `libs/data-models/schemas/facts.py`                 |
| 2.5  | Define Dimension table schemas in Python       | `libs/data-models/schemas/dimensions.py`            |
| 2.6  | Write SQL DDL scripts                          | `libs/data-models/ddl/01-06_*.sql`                  |
| 2.7  | Define partition specs per table               | `libs/data-models/partitioning/partition_specs.py`  |

#### Phase 3: Table Creation & Seeding (Week 2)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 3.1  | Create namespaces (bronze, silver, gold, dim)  | `libs/data-models/ddl/01_create_namespaces.sql`     |
| 3.2  | Create all tables via initial migration        | `libs/data-models/migrations/V001__initial_schema.py` |
| 3.3  | Seed `dim_time` (2 years of minute-level data) | `libs/data-models/seed/seed_dim_time.py`            |
| 3.4  | Seed `dim_exchange` with reference data        | `libs/data-models/seed/seed_dim_exchange.py`        |

#### Phase 4: SCD Implementation (Week 2-3)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 4.1  | Implement surrogate key generator              | `libs/data-models/scd/surrogate_keys.py`            |
| 4.2  | Implement SCD Type 1 logic                     | `libs/data-models/scd/scd_type1.py`                 |
| 4.3  | Implement SCD Type 2 logic                     | `libs/data-models/scd/scd_type2.py`                 |
| 4.4  | Implement SCD Type 3 logic                     | `libs/data-models/scd/scd_type3.py`                 |
| 4.5  | Unit test all SCD implementations              | `tests/data_models/test_scd_type*.py`               |

#### Phase 5: Data Pipeline Integration (Week 3-4)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 5.1  | Build Bronze writer (Kafka -> Iceberg)         | `services/storage/bronze_writer.py`                 |
| 5.2  | Build Silver processor (dedup, enrich, cast)   | `services/storage/silver_processor.py`              |
| 5.3  | Build Dimension loader (SCD orchestration)     | `services/storage/dimension_loader.py`              |
| 5.4  | Build Fact loader (Silver + Dims -> Facts)     | `services/storage/fact_loader.py`                   |
| 5.5  | Build Gold aggregator (Silver -> Gold metrics) | `services/storage/gold_aggregator.py`               |

#### Phase 6: Maintenance & Testing (Week 4)

| Step | Task                                           | Output File(s)                                      |
|------|------------------------------------------------|-----------------------------------------------------|
| 6.1  | Implement compaction jobs                      | `libs/data-models/maintenance/compaction.py`        |
| 6.2  | Implement snapshot expiration                  | `libs/data-models/maintenance/snapshot_expiration.py` |
| 6.3  | Implement orphan file cleanup                  | `libs/data-models/maintenance/orphan_cleanup.py`    |
| 6.4  | Integration tests (end-to-end flow)            | `tests/data_models/test_data_integrity.py`          |
| 6.5  | Register maintenance jobs in Dagster           | Dagster assets/schedules (separate module)          |

---

## 9. Testing Strategy

### 9.1 Schema Validation Tests

**File:** `tests/data_models/test_schemas.py`

Validate that all table schemas match their specifications.

```python
class TestSchemaValidation:
    """Verify Iceberg table schemas match expected definitions."""

    def test_bronze_raw_trades_schema(self, test_catalog):
        """Validate bronze.raw_trades has all expected columns and types."""
        table = test_catalog.load_table("bronze.raw_trades")
        schema = table.schema()

        assert schema.find_field("trade_id").field_type == StringType()
        assert schema.find_field("price").field_type == DoubleType()
        assert schema.find_field("quantity").field_type == LongType()
        assert schema.find_field("timestamp").field_type == TimestampType()
        # ... validate all columns

    def test_all_tables_use_format_version_2(self, test_catalog, all_table_names):
        """Every table must use format version 2 for row-level deletes."""
        for table_name in all_table_names:
            table = test_catalog.load_table(table_name)
            assert table.metadata.format_version == 2

    def test_all_tables_use_parquet(self, test_catalog, all_table_names):
        """Every table must write in Parquet format."""
        for table_name in all_table_names:
            table = test_catalog.load_table(table_name)
            assert table.properties.get("write.format.default") == "parquet"

    def test_all_tables_use_zstd(self, test_catalog, all_table_names):
        """Every table must use zstd compression."""
        for table_name in all_table_names:
            table = test_catalog.load_table(table_name)
            assert table.properties.get("write.parquet.compression-codec") == "zstd"

    def test_partition_specs(self, test_catalog):
        """Validate partition specs match documented strategy."""
        table = test_catalog.load_table("silver.trades")
        spec = table.spec()
        field_names = [f.name for f in spec.fields]
        assert "trade_timestamp_day" in field_names
        assert "symbol_bucket" in field_names
```

### 9.2 SCD Logic Tests

**File:** `tests/data_models/test_scd_type2.py` (representative example)

```python
class TestSCDType2:
    """Test SCD Type 2 expire-and-insert logic."""

    def test_new_record_inserted_with_current_flag(self, scd2_handler, dim_symbol_table):
        """A brand new symbol gets is_current=True and expiry_date=9999-12-31."""
        incoming = pd.DataFrame([{
            "symbol": "NEWCO",
            "company_name": "New Company Inc",
            "sector": "Technology",
            "industry": "Software",
            "market_cap_category": "SMALL",
            "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, incoming)

        result = dim_symbol_table.scan(
            row_filter=EqualTo("symbol", "NEWCO")
        ).to_pandas()

        assert len(result) == 1
        assert result.iloc[0]["is_current"] == True
        assert result.iloc[0]["expiry_date"] == date(9999, 12, 31)

    def test_changed_record_expires_old_inserts_new(self, scd2_handler, dim_symbol_table):
        """When sector changes, old record is expired and new version created."""
        # Setup: insert initial record
        initial = pd.DataFrame([{
            "symbol": "AAPL",
            "company_name": "Apple Inc",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "market_cap_category": "MEGA",
            "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, initial)

        # Act: update sector
        updated = pd.DataFrame([{
            "symbol": "AAPL",
            "company_name": "Apple Inc",
            "sector": "Consumer Discretionary",  # Changed
            "industry": "Consumer Electronics",
            "market_cap_category": "MEGA",
            "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, updated)

        # Assert
        all_records = dim_symbol_table.scan(
            row_filter=EqualTo("symbol", "AAPL")
        ).to_pandas()

        assert len(all_records) == 2  # Old expired + new current

        current = all_records[all_records["is_current"] == True]
        expired = all_records[all_records["is_current"] == False]

        assert len(current) == 1
        assert current.iloc[0]["sector"] == "Consumer Discretionary"
        assert current.iloc[0]["expiry_date"] == date(9999, 12, 31)

        assert len(expired) == 1
        assert expired.iloc[0]["sector"] == "Technology"
        assert expired.iloc[0]["expiry_date"] < date(9999, 12, 31)

    def test_unchanged_record_not_duplicated(self, scd2_handler, dim_symbol_table):
        """Re-loading identical data should not create duplicate records."""
        record = pd.DataFrame([{
            "symbol": "MSFT",
            "company_name": "Microsoft Corp",
            "sector": "Technology",
            "industry": "Software",
            "market_cap_category": "MEGA",
            "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, record)
        scd2_handler.apply(dim_symbol_table, record)  # Same data again

        result = dim_symbol_table.scan(
            row_filter=EqualTo("symbol", "MSFT")
        ).to_pandas()

        assert len(result) == 1  # No duplicate

    def test_surrogate_key_uniqueness(self, scd2_handler, dim_symbol_table):
        """Each version must have a unique surrogate key."""
        initial = pd.DataFrame([{
            "symbol": "GOOG", "company_name": "Alphabet Inc",
            "sector": "Communication Services", "industry": "Internet",
            "market_cap_category": "MEGA", "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, initial)

        updated = pd.DataFrame([{
            "symbol": "GOOG", "company_name": "Alphabet Inc",
            "sector": "Technology", "industry": "Internet",  # sector changed
            "market_cap_category": "MEGA", "listing_exchange": "NASDAQ",
        }])
        scd2_handler.apply(dim_symbol_table, updated)

        result = dim_symbol_table.scan(
            row_filter=EqualTo("symbol", "GOOG")
        ).to_pandas()

        surrogate_keys = result["symbol_key"].tolist()
        assert len(surrogate_keys) == len(set(surrogate_keys))  # All unique
```

### 9.3 Data Integrity Tests

**File:** `tests/data_models/test_data_integrity.py`

```python
class TestReferentialIntegrity:
    """Verify foreign key relationships between fact and dimension tables."""

    def test_fact_trades_symbol_key_exists_in_dim(self, test_catalog):
        """Every symbol_key in fact_trades must exist in dim_symbol."""
        fact = test_catalog.load_table("fact_trades").scan().to_pandas()
        dim = test_catalog.load_table("dim_symbol").scan().to_pandas()

        fact_keys = set(fact["symbol_key"].unique())
        dim_keys = set(dim["symbol_key"].unique())

        orphaned = fact_keys - dim_keys
        assert len(orphaned) == 0, f"Orphaned symbol_keys in fact_trades: {orphaned}"

    def test_fact_trades_trader_key_exists_in_dim(self, test_catalog):
        """Every trader_key in fact_trades must exist in dim_trader."""
        fact = test_catalog.load_table("fact_trades").scan().to_pandas()
        dim = test_catalog.load_table("dim_trader").scan().to_pandas()

        fact_keys = set(fact["trader_key"].unique())
        dim_keys = set(dim["trader_key"].unique())

        orphaned = fact_keys - dim_keys
        assert len(orphaned) == 0, f"Orphaned trader_keys in fact_trades: {orphaned}"

    def test_fact_trades_time_key_exists_in_dim(self, test_catalog):
        """Every time_key in fact_trades must exist in dim_time."""
        fact = test_catalog.load_table("fact_trades").scan().to_pandas()
        dim = test_catalog.load_table("dim_time").scan().to_pandas()

        fact_keys = set(fact["time_key"].unique())
        dim_keys = set(dim["time_key"].unique())

        orphaned = fact_keys - dim_keys
        assert len(orphaned) == 0, f"Orphaned time_keys in fact_trades: {orphaned}"

    def test_fact_trades_exchange_key_exists_in_dim(self, test_catalog):
        """Every exchange_key in fact_trades must exist in dim_exchange."""
        fact = test_catalog.load_table("fact_trades").scan().to_pandas()
        dim = test_catalog.load_table("dim_exchange").scan().to_pandas()

        fact_keys = set(fact["exchange_key"].unique())
        dim_keys = set(dim["exchange_key"].unique())

        orphaned = fact_keys - dim_keys
        assert len(orphaned) == 0, f"Orphaned exchange_keys in fact_trades: {orphaned}"

    def test_no_null_foreign_keys_in_facts(self, test_catalog):
        """Fact tables must not have NULL foreign keys."""
        fact = test_catalog.load_table("fact_trades").scan().to_pandas()

        for fk_col in ["symbol_key", "trader_key", "time_key", "exchange_key"]:
            null_count = fact[fk_col].isnull().sum()
            assert null_count == 0, f"{fk_col} has {null_count} NULL values"


class TestDimensionIntegrity:
    """Verify dimension table data quality."""

    def test_scd2_exactly_one_current_per_natural_key(self, test_catalog):
        """Each natural key must have exactly one is_current=True record."""
        for table_name, nk in [("dim_symbol", "symbol"), ("dim_trader", "trader_id")]:
            dim = test_catalog.load_table(table_name).scan().to_pandas()
            current = dim[dim["is_current"] == True]
            duplicates = current[current.duplicated(subset=[nk], keep=False)]
            assert len(duplicates) == 0, (
                f"{table_name}: multiple current records for: "
                f"{duplicates[nk].unique().tolist()}"
            )

    def test_scd2_no_gaps_in_effective_dates(self, test_catalog):
        """For SCD2 dims, effective/expiry date ranges must be contiguous per natural key."""
        for table_name, nk in [("dim_symbol", "symbol"), ("dim_trader", "trader_id")]:
            dim = test_catalog.load_table(table_name).scan().to_pandas()
            for key_value in dim[nk].unique():
                records = dim[dim[nk] == key_value].sort_values("effective_date")
                for i in range(len(records) - 1):
                    current_expiry = records.iloc[i]["expiry_date"]
                    next_effective = records.iloc[i + 1]["effective_date"]
                    # Next record's effective_date should be expiry_date + 1 day
                    expected = current_expiry + timedelta(days=1)
                    assert next_effective == expected, (
                        f"Gap in {table_name} for {nk}={key_value}: "
                        f"expiry={current_expiry}, next_effective={next_effective}"
                    )

    def test_dim_time_completeness(self, test_catalog):
        """dim_time must have one row per minute for every day in the range."""
        dim = test_catalog.load_table("dim_time").scan().to_pandas()
        dates = dim["date"].unique()
        for d in dates:
            day_records = dim[dim["date"] == d]
            # 24 hours * 60 minutes = 1440 rows per day
            assert len(day_records) == 1440, (
                f"dim_time has {len(day_records)} rows for {d}, expected 1440"
            )

    def test_scd3_previous_value_consistency(self, test_catalog):
        """For SCD3, if previous_X is not null, change_date must also be not null."""
        dim = test_catalog.load_table("dim_account").scan().to_pandas()

        for prev_col, date_col in [
            ("previous_tier", "tier_change_date"),
            ("previous_status", "status_change_date"),
        ]:
            has_previous = dim[dim[prev_col].notna()]
            missing_date = has_previous[has_previous[date_col].isna()]
            assert len(missing_date) == 0, (
                f"dim_account: {len(missing_date)} rows have {prev_col} "
                f"but missing {date_col}"
            )
```

### 9.4 Test Fixtures

**File:** `tests/data_models/conftest.py`

```python
import pytest
from pyiceberg.catalog import load_catalog


@pytest.fixture(scope="session")
def test_catalog():
    """Create a test catalog pointing to a test namespace in MinIO."""
    catalog = load_catalog(
        "rest",
        **{
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "admin_secret",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )
    # Create test namespaces
    for ns in ["test_bronze", "test_silver", "test_gold", "test_dim"]:
        catalog.create_namespace(ns, ignore_existing=True)

    yield catalog

    # Cleanup: drop test tables and namespaces
    for ns in ["test_bronze", "test_silver", "test_gold", "test_dim"]:
        for table in catalog.list_tables(ns):
            catalog.drop_table(table)
        catalog.drop_namespace(ns)


@pytest.fixture
def all_table_names():
    """List of all table identifiers in the project."""
    return [
        "bronze.raw_trades",
        "bronze.raw_orderbook",
        "bronze.raw_marketdata",
        "silver.trades",
        "silver.orderbook_snapshots",
        "silver.market_data",
        "silver.trader_activity",
        "gold.daily_trading_summary",
        "gold.trader_performance",
        "gold.market_overview",
        "gold.portfolio_positions",
        "gold.trade_analytics",
        "dim_symbol",
        "dim_trader",
        "dim_exchange",
        "dim_time",
        "dim_account",
        "fact_trades",
        "fact_orderbook",
    ]


@pytest.fixture
def sample_trade_data():
    """Sample trade data for testing."""
    import pandas as pd
    from datetime import datetime

    return pd.DataFrame([
        {
            "trade_id": "T001",
            "symbol": "AAPL",
            "price": 185.50,
            "quantity": 100,
            "trade_type": "BUY",
            "order_type": "LIMIT",
            "trader_id": "TR001",
            "exchange": "NASDAQ",
            "timestamp": datetime(2025, 1, 15, 10, 30, 0),
        },
        {
            "trade_id": "T002",
            "symbol": "MSFT",
            "price": 420.75,
            "quantity": 50,
            "trade_type": "SELL",
            "order_type": "MARKET",
            "trader_id": "TR002",
            "exchange": "NASDAQ",
            "timestamp": datetime(2025, 1, 15, 10, 31, 0),
        },
    ])
```

### 9.5 Test Execution

```bash
# Run all storage/data-model tests
pytest tests/data_models/ -v

# Run only schema validation tests
pytest tests/data_models/test_schemas.py -v

# Run only SCD tests
pytest tests/data_models/test_scd_type1.py tests/data_models/test_scd_type2.py tests/data_models/test_scd_type3.py -v

# Run only data integrity tests
pytest tests/data_models/test_data_integrity.py -v

# Run with coverage
pytest tests/data_models/ --cov=libs/data_models --cov-report=html
```

**Note:** Integration tests require Docker services (MinIO + Iceberg REST catalog) to be running. Use `docker compose -f infra/docker/docker-compose.storage.yml up -d` before running the test suite.

---

## Appendix: Key Dependencies

| Library / Tool         | Version  | Purpose                                    |
|------------------------|----------|--------------------------------------------|
| `pyiceberg`            | >= 0.7.0 | Python client for Iceberg tables           |
| `pyarrow`              | >= 14.0  | Arrow / Parquet read/write engine          |
| `pandas`               | >= 2.0   | DataFrame operations for SCD logic         |
| `minio`                | latest   | S3-compatible object storage               |
| `tabulario/iceberg-rest` | latest | Iceberg REST catalog server                |
| `duckdb`               | >= 1.0   | In-memory SQL for Gold aggregations        |
| `pytest`               | >= 8.0   | Test framework                             |
