# 06 - Data Quality & Validation

## Overview

This document defines the implementation plan for the Data Quality & Validation layer of the stock market trade data pipeline. The system validates data across the Bronze (raw), Silver (cleaned), and Gold (aggregated) layers of the Iceberg lakehouse, using Great Expectations integrated with Dagster via the `dagster-ge` library.

The quality framework enforces five dimensions of data quality: **Completeness**, **Accuracy**, **Consistency**, **Timeliness**, and **Uniqueness** -- ensuring that every trade record flowing through the pipeline meets strict correctness guarantees before it is promoted to downstream consumers.

---

## Table of Contents

1. [Framework Selection: Great Expectations](#1-framework-selection-great-expectations)
2. [Dagster Integration](#2-dagster-integration)
3. [Expectation Suites by Layer](#3-expectation-suites-by-layer)
4. [Timeliness (Freshness SLAs)](#4-timeliness-freshness-slas)
5. [Deduplication Strategy](#5-deduplication-strategy)
6. [Data Quality Scoring](#6-data-quality-scoring)
7. [Alerting on Quality Failures](#7-alerting-on-quality-failures)
8. [Data Docs (Auto-Generated Documentation)](#8-data-docs-auto-generated-documentation)
9. [Implementation Steps](#9-implementation-steps)
10. [Testing Strategy](#10-testing-strategy)

---

## 1. Framework Selection: Great Expectations

### 1.1 Why Great Expectations over Apache Deequ

| Criterion | Great Expectations | Apache Deequ |
|---|---|---|
| **Language** | Native Python | Scala (JVM), Python wrapper is limited |
| **Dagster Integration** | First-class via `dagster-ge` | No official integration |
| **Expectation Library** | 300+ built-in expectations | Smaller set of analyzers/checks |
| **Documentation** | Auto-generated Data Docs | Manual reporting |
| **DataFrame Support** | Pandas, Spark, SQL | Spark only |
| **Community** | Very active, well-maintained | Active but Spark-centric |
| **Monorepo Fit** | Pure Python, pip installable | Requires JVM/Spark runtime |
| **Custom Expectations** | Easy Python class extension | Scala-heavy customization |

**Decision**: Great Expectations is the clear choice for this project. It is native Python (consistent with the monorepo language), has an excellent Dagster integration (`dagster-ge`), provides a rich library of 300+ expectations, auto-generates Data Docs for stakeholder visibility, supports both Pandas and Spark DataFrames, and has an active, well-maintained community. Apache Deequ's reliance on the JVM/Spark runtime and lack of official Dagster integration make it a poor fit.

### 1.2 Great Expectations Project Setup

The Great Expectations project lives within the data quality service directory:

```
services/data-quality/
├── great_expectations/
│   ├── great_expectations.yml          # Data Context configuration
│   ├── expectations/
│   │   ├── bronze_trades_suite.json    # Bronze layer expectations
│   │   ├── silver_trades_suite.json    # Silver layer expectations
│   │   ├── gold_daily_summary_suite.json  # Gold layer expectations
│   │   └── cross_layer_suite.json      # Cross-source validation
│   ├── checkpoints/
│   │   ├── bronze_checkpoint.yml
│   │   ├── silver_checkpoint.yml
│   │   └── gold_checkpoint.yml
│   ├── plugins/
│   │   └── custom_expectations/
│   │       ├── __init__.py
│   │       ├── expect_vwap_between_high_and_low.py
│   │       ├── expect_high_gte_open_and_close.py
│   │       └── expect_trade_count_matches_source.py
│   ├── uncommitted/
│   │   ├── config_variables.yml        # Environment-specific secrets
│   │   └── data_docs/                  # Generated Data Docs (not committed)
│   └── .gitignore
├── pyproject.toml
├── requirements.txt
└── tests/
    ├── __init__.py
    ├── test_bronze_expectations.py
    ├── test_silver_expectations.py
    ├── test_gold_expectations.py
    └── fixtures/
        ├── valid_trades.csv
        ├── invalid_trades_nulls.csv
        ├── invalid_trades_types.csv
        └── duplicate_trades.csv
```

### 1.3 Data Context Configuration

```yaml
# services/data-quality/great_expectations/great_expectations.yml

config_version: 3.0

datasources:
  iceberg_bronze:
    class_name: Datasource
    execution_engine:
      class_name: PandasExecutionEngine
    data_connectors:
      bronze_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - batch_id
          - run_date

  iceberg_silver:
    class_name: Datasource
    execution_engine:
      class_name: PandasExecutionEngine
    data_connectors:
      silver_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - batch_id
          - run_date

  iceberg_gold:
    class_name: Datasource
    execution_engine:
      class_name: PandasExecutionEngine
    data_connectors:
      gold_connector:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - batch_id
          - run_date

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/validations/

  evaluation_parameter_store:
    class_name: EvaluationParameterStore

  checkpoint_store:
    class_name: CheckpointStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: checkpoints/

data_docs_sites:
  local_site:
    class_name: SiteBuilder
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: uncommitted/data_docs/local_site/
    site_index_builder:
      class_name: DefaultSiteIndexBuilder

config_variables_file_path: uncommitted/config_variables.yml
```

### 1.4 Python Dependencies

```
# services/data-quality/requirements.txt
great-expectations>=0.18.0,<1.0.0
dagster-ge>=0.23.0
pandas>=2.0.0
pyarrow>=14.0.0
pyiceberg>=0.6.0
```

---

## 2. Dagster Integration

### 2.1 dagster-ge Integration Setup

The `dagster-ge` library provides two primary integration patterns:

1. **`ge_validation_op_factory`** -- Creates Dagster ops that run GE checkpoint validations.
2. **Asset Checks (Dagster 1.5+)** -- Inline quality checks attached directly to asset definitions.

Both patterns are used in this project: ops for batch validation workflows, and asset checks for inline guardrails.

```python
# services/dagster-orchestrator/dagster_orchestrator/resources/ge_resource.py

from dagster import resource, InitResourceContext
from dagster_ge import ge_data_context

import os

GE_ROOT_DIR = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "data-quality", "great_expectations"
)

@resource(description="Great Expectations Data Context resource")
def ge_data_context_resource(context: InitResourceContext):
    return ge_data_context.configured({"ge_root_dir": GE_ROOT_DIR})
```

### 2.2 Using ge_validation_op_factory for Validation Ops

`ge_validation_op_factory` creates Dagster ops that execute a Great Expectations checkpoint. This is the preferred approach for standalone validation steps in a job graph.

```python
# services/dagster-orchestrator/dagster_orchestrator/ops/quality_ops.py

from dagster_ge import ge_validation_op_factory

# Creates a Dagster op that runs the bronze checkpoint
validate_bronze_trades_op = ge_validation_op_factory(
    name="validate_bronze_trades",
    datasource_name="iceberg_bronze",
    suite_name="bronze_trades_suite",
    validation_operator_name="action_list_operator",
)

validate_silver_trades_op = ge_validation_op_factory(
    name="validate_silver_trades",
    datasource_name="iceberg_silver",
    suite_name="silver_trades_suite",
    validation_operator_name="action_list_operator",
)

validate_gold_summary_op = ge_validation_op_factory(
    name="validate_gold_daily_summary",
    datasource_name="iceberg_gold",
    suite_name="gold_daily_summary_suite",
    validation_operator_name="action_list_operator",
)
```

Usage in a Dagster job:

```python
# services/dagster-orchestrator/dagster_orchestrator/jobs/quality_jobs.py

from dagster import job, In, Nothing
from ..ops.quality_ops import (
    validate_bronze_trades_op,
    validate_silver_trades_op,
    validate_gold_summary_op,
)

@job(
    resource_defs={"ge_data_context": ge_data_context_resource},
    description="Run all data quality validations across layers",
)
def full_quality_validation_job():
    bronze_result = validate_bronze_trades_op()
    silver_result = validate_silver_trades_op()
    gold_result = validate_gold_summary_op()
```

### 2.3 Asset Checks (Dagster 1.5+) for Inline Quality Checks

Asset checks are the modern, preferred approach for quality validations tied to specific assets. They run automatically after asset materialization and appear in the Dagster UI alongside the asset.

```python
# services/dagster-orchestrator/dagster_orchestrator/assets/quality_checks.py

from dagster import (
    asset_check,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
)
import great_expectations as gx
import pandas as pd


@asset_check(
    asset=AssetKey("bronze_trades"),
    description="Validate bronze trades against the bronze_trades_suite",
)
def bronze_trades_quality_check(context, bronze_trades: pd.DataFrame) -> AssetCheckResult:
    """Run Great Expectations bronze suite against the materialized bronze trades."""
    ge_context = gx.get_context(
        context_root_dir="services/data-quality/great_expectations"
    )

    # Create a batch from the DataFrame
    batch_request = ge_context.get_datasource("iceberg_bronze").get_asset(
        "bronze_connector"
    ).build_batch_request(dataframe=bronze_trades)

    # Run the checkpoint
    result = ge_context.run_checkpoint(
        checkpoint_name="bronze_checkpoint",
        batch_request=batch_request,
    )

    success = result.success
    stats = result.statistics
    failed_expectations = [
        r.expectation_config.expectation_type
        for r in result.results
        if not r.success
    ]

    return AssetCheckResult(
        passed=success,
        severity=AssetCheckSeverity.ERROR if not success else AssetCheckSeverity.WARN,
        metadata={
            "evaluated_expectations": stats["evaluated_expectations"],
            "successful_expectations": stats["successful_expectations"],
            "unsuccessful_expectations": stats["unsuccessful_expectations"],
            "success_percent": stats["success_percent"],
            "failed_checks": str(failed_expectations),
        },
    )


@asset_check(
    asset=AssetKey("silver_trades"),
    description="Validate silver trades against the silver_trades_suite",
)
def silver_trades_quality_check(context, silver_trades: pd.DataFrame) -> AssetCheckResult:
    """Run Great Expectations silver suite against the materialized silver trades."""
    ge_context = gx.get_context(
        context_root_dir="services/data-quality/great_expectations"
    )

    batch_request = ge_context.get_datasource("iceberg_silver").get_asset(
        "silver_connector"
    ).build_batch_request(dataframe=silver_trades)

    result = ge_context.run_checkpoint(
        checkpoint_name="silver_checkpoint",
        batch_request=batch_request,
    )

    success = result.success
    stats = result.statistics
    failed_expectations = [
        r.expectation_config.expectation_type
        for r in result.results
        if not r.success
    ]

    return AssetCheckResult(
        passed=success,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "evaluated_expectations": stats["evaluated_expectations"],
            "successful_expectations": stats["successful_expectations"],
            "unsuccessful_expectations": stats["unsuccessful_expectations"],
            "success_percent": stats["success_percent"],
            "failed_checks": str(failed_expectations),
        },
    )


@asset_check(
    asset=AssetKey("gold_daily_summary"),
    description="Validate gold daily summary against the gold_daily_summary_suite",
)
def gold_daily_summary_quality_check(context, gold_daily_summary: pd.DataFrame) -> AssetCheckResult:
    """Run Great Expectations gold suite against the materialized gold daily summary."""
    ge_context = gx.get_context(
        context_root_dir="services/data-quality/great_expectations"
    )

    batch_request = ge_context.get_datasource("iceberg_gold").get_asset(
        "gold_connector"
    ).build_batch_request(dataframe=gold_daily_summary)

    result = ge_context.run_checkpoint(
        checkpoint_name="gold_checkpoint",
        batch_request=batch_request,
    )

    success = result.success
    stats = result.statistics

    return AssetCheckResult(
        passed=success,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "evaluated_expectations": stats["evaluated_expectations"],
            "successful_expectations": stats["successful_expectations"],
            "unsuccessful_expectations": stats["unsuccessful_expectations"],
            "success_percent": stats["success_percent"],
        },
    )
```

### 2.4 Validation as Assets vs Validation as Ops

| Approach | When to Use | Pros | Cons |
|---|---|---|---|
| **Asset Checks** | Inline validation tied to a specific asset | Visible in asset graph, runs automatically, blocks downstream on failure | Coupled to asset definition |
| **Validation Ops** | Standalone validation jobs, cross-layer checks | Decoupled, can orchestrate independently | Not visible on asset graph |

**Recommendation**: Use **Asset Checks** as the primary validation mechanism for each layer. Use **Validation Ops** for cross-layer validations and ad-hoc quality audit jobs.

### 2.5 Triggering Validations After Asset Materialization

Asset checks run automatically when attached to an asset. For op-based validations, use sensors:

```python
# services/dagster-orchestrator/dagster_orchestrator/sensors/quality_sensors.py

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    AssetKey,
    EventLogEntry,
    DagsterEventType,
)


@sensor(
    job_name="cross_layer_validation_job",
    minimum_interval_seconds=60,
    description="Trigger cross-layer validation when gold layer materializes",
)
def cross_layer_quality_sensor(context: SensorEvaluationContext):
    """Watch for gold_daily_summary materialization and trigger cross-layer validation."""
    events = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey("gold_daily_summary"),
            after_cursor=context.cursor,
        ),
        limit=1,
    )

    if events:
        context.update_cursor(str(events[0].storage_id))
        yield RunRequest(
            run_key=f"cross_layer_{events[0].storage_id}",
            run_config={},
        )
```

---

## 3. Expectation Suites by Layer

### 3.1 Bronze Layer Validations

**Suite**: `bronze_trades_suite`
**Purpose**: Validate raw ingested data for basic completeness, schema conformity, and elementary validity. Bronze accepts duplicates and allows generous value ranges -- the goal is to catch corrupt or structurally broken data.

```python
# Programmatic suite definition for reference
# Actual suite stored as JSON in expectations/bronze_trades_suite.json

import great_expectations as gx

context = gx.get_context()
suite = context.add_expectation_suite("bronze_trades_suite")

# --- COMPLETENESS ---
# Critical columns must not be null
for column in ["trade_id", "symbol", "timestamp", "price", "quantity"]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column=column,
            meta={"dimension": "completeness", "severity": "critical"},
        )
    )

# Every batch must contain at least one row (non-empty ingestion)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1,
        meta={"dimension": "completeness", "severity": "critical"},
    )
)

# --- SCHEMA ---
# Column order and presence must match the expected schema
suite.add_expectation(
    gx.expectations.ExpectTableColumnsToMatchOrderedList(
        column_list=[
            "trade_id",
            "symbol",
            "timestamp",
            "price",
            "quantity",
            "trade_type",
            "status",
            "exchange",
            "bid_price",
            "ask_price",
            "ingestion_timestamp",
        ],
        meta={"dimension": "schema", "severity": "critical"},
    )
)

# Type checks
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeOfType(
        column="price",
        type_="float",
        meta={"dimension": "schema", "severity": "critical"},
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeOfType(
        column="quantity",
        type_="int",
        meta={"dimension": "schema", "severity": "critical"},
    )
)

# --- BASIC VALIDITY ---
# trade_type must be BUY or SELL
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="trade_type",
        value_set=["BUY", "SELL"],
        meta={"dimension": "validity", "severity": "critical"},
    )
)

# status must be one of the known trade statuses
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"],
        meta={"dimension": "validity", "severity": "critical"},
    )
)
```

**Expected JSON output** (`expectations/bronze_trades_suite.json`):

```json
{
  "expectation_suite_name": "bronze_trades_suite",
  "ge_cloud_id": null,
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "trade_id" },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "symbol" },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "timestamp" },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "price" },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "quantity" },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": { "min_value": 1 },
      "meta": { "dimension": "completeness", "severity": "critical" }
    },
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": [
          "trade_id", "symbol", "timestamp", "price", "quantity",
          "trade_type", "status", "exchange", "bid_price", "ask_price",
          "ingestion_timestamp"
        ]
      },
      "meta": { "dimension": "schema", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_be_of_type",
      "kwargs": { "column": "price", "type_": "float" },
      "meta": { "dimension": "schema", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_be_of_type",
      "kwargs": { "column": "quantity", "type_": "int" },
      "meta": { "dimension": "schema", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": { "column": "trade_type", "value_set": ["BUY", "SELL"] },
      "meta": { "dimension": "validity", "severity": "critical" }
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "status",
        "value_set": ["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"]
      },
      "meta": { "dimension": "validity", "severity": "critical" }
    }
  ],
  "meta": {
    "great_expectations_version": "0.18.x",
    "layer": "bronze",
    "description": "Validates raw ingested trade data for completeness, schema conformity, and basic validity."
  }
}
```

### 3.2 Silver Layer Validations

**Suite**: `silver_trades_suite`
**Purpose**: Validate cleaned, deduplicated, and enriched data. Silver is the "source of truth" layer, so validations are strict: uniqueness, accuracy ranges, referential integrity, and consistency.

```python
# Programmatic suite definition for reference

suite = context.add_expectation_suite("silver_trades_suite")

# --- UNIQUENESS ---
# trade_id must be globally unique (post-dedup)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(
        column="trade_id",
        meta={"dimension": "uniqueness", "severity": "critical"},
    )
)

# Compound uniqueness: (trade_id, timestamp) pair must be unique
suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["trade_id", "timestamp"],
        meta={"dimension": "uniqueness", "severity": "critical"},
    )
)

# --- ACCURACY ---
# Price must be in a reasonable range for stock trades
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="price",
        min_value=0.01,
        max_value=100000.00,
        meta={"dimension": "accuracy", "severity": "critical"},
    )
)

# Quantity must be a positive integer within exchange limits
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="quantity",
        min_value=1,
        max_value=1000000,
        meta={"dimension": "accuracy", "severity": "critical"},
    )
)

# Ask price must be greater than bid price (market microstructure rule)
suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="ask_price",
        column_B="bid_price",
        or_equal=False,
        meta={"dimension": "accuracy", "severity": "warning"},
    )
)

# --- REFERENTIAL INTEGRITY ---
# Symbol must be from the known symbols master list
# (known_symbols_list loaded from reference data at runtime)
KNOWN_SYMBOLS = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA",
    "JPM", "BAC", "WFC", "GS", "MS", "C",
    "JNJ", "PFE", "UNH", "MRK", "ABBV",
    # ... extended from reference data
]

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="symbol",
        value_set=KNOWN_SYMBOLS,
        meta={"dimension": "referential_integrity", "severity": "critical"},
    )
)

KNOWN_EXCHANGES = ["NYSE", "NASDAQ", "AMEX", "ARCA", "BATS", "IEX"]

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="exchange",
        value_set=KNOWN_EXCHANGES,
        meta={"dimension": "referential_integrity", "severity": "critical"},
    )
)

# --- CONSISTENCY ---
# trade_id must match UUID v4 pattern
UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="trade_id",
        regex=UUID_PATTERN,
        meta={"dimension": "consistency", "severity": "critical"},
    )
)

# timestamp must be parseable as a datetime
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeDateutilParseable(
        column="timestamp",
        meta={"dimension": "consistency", "severity": "critical"},
    )
)
```

### 3.3 Gold Layer Validations

**Suite**: `gold_daily_summary_suite`
**Purpose**: Validate aggregated daily trade summaries. Checks business rules for OHLCV (Open, High, Low, Close, Volume) data, statistical distributions, and completeness of symbol coverage.

```python
# Programmatic suite definition for reference

suite = context.add_expectation_suite("gold_daily_summary_suite")

# --- COMPLETENESS ---
# All aggregated columns must be present and non-null
AGGREGATED_COLUMNS = [
    "symbol", "trade_date", "daily_open", "daily_high", "daily_low",
    "daily_close", "daily_volume", "trade_count", "vwap",
]

for column in AGGREGATED_COLUMNS:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column=column,
            meta={"dimension": "completeness", "severity": "critical"},
        )
    )

# All actively-traded symbols must appear in the daily summary
# (checked via a custom expectation or cross-layer validation)
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=len(KNOWN_SYMBOLS) * 0.9,  # Allow 10% missing on light days
        meta={"dimension": "completeness", "severity": "warning"},
    )
)

# --- BUSINESS RULES ---
# Daily volume must be positive
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="daily_volume",
        min_value=1,
        strict_min=False,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

# VWAP must be between daily low and daily high
suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="vwap",
        column_B="daily_low",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="daily_high",
        column_B="vwap",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

# daily_high >= daily_open AND daily_high >= daily_close
suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="daily_high",
        column_B="daily_open",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="daily_high",
        column_B="daily_close",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

# daily_low <= daily_open AND daily_low <= daily_close
suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="daily_open",
        column_B="daily_low",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="daily_close",
        column_B="daily_low",
        or_equal=True,
        meta={"dimension": "business_rule", "severity": "critical"},
    )
)

# --- DISTRIBUTION ---
# Mean price should be in a reasonable range (detect wild outliers)
suite.add_expectation(
    gx.expectations.ExpectColumnMeanToBeBetween(
        column="daily_close",
        min_value=1.0,
        max_value=10000.0,
        meta={"dimension": "distribution", "severity": "warning"},
    )
)

# Standard deviation of volume should be within expected bounds
# (detect anomalous concentration or absence of trading)
suite.add_expectation(
    gx.expectations.ExpectColumnStdevToBeBetween(
        column="daily_volume",
        min_value=0.0,
        max_value=100000000.0,
        meta={"dimension": "distribution", "severity": "warning"},
    )
)

# Mean trade count per symbol should be reasonable
suite.add_expectation(
    gx.expectations.ExpectColumnMeanToBeBetween(
        column="trade_count",
        min_value=1,
        max_value=1000000,
        meta={"dimension": "distribution", "severity": "warning"},
    )
)
```

### 3.4 Cross-Source Validation

Cross-source validations compare data across layers to detect data loss, duplication errors, or aggregation mistakes. These are implemented as standalone Dagster ops (not asset checks) since they span multiple assets.

```python
# services/dagster-orchestrator/dagster_orchestrator/ops/cross_layer_validation.py

from dagster import op, Out, Output, AssetKey, OpExecutionContext
import pandas as pd


@op(
    description="Validate that trade counts in Silver match Bronze after dedup",
    required_resource_keys={"iceberg_catalog"},
)
def validate_bronze_silver_trade_count(context: OpExecutionContext):
    """
    Cross-layer check: Silver trade count should equal Bronze trade count
    minus known duplicate removals.
    """
    catalog = context.resources.iceberg_catalog

    bronze_count = catalog.load_table("bronze.trades").scan().to_arrow().num_rows
    silver_count = catalog.load_table("silver.trades").scan().to_arrow().num_rows

    # Bronze stores duplicates; Silver deduplicates.
    # The ratio should be within a known tolerance.
    # Typically: silver_count <= bronze_count
    # And silver_count >= bronze_count * 0.7 (at most 30% duplicates expected)

    dedup_ratio = silver_count / bronze_count if bronze_count > 0 else 0
    is_valid = 0.70 <= dedup_ratio <= 1.00

    context.log.info(
        f"Bronze count: {bronze_count}, Silver count: {silver_count}, "
        f"Dedup ratio: {dedup_ratio:.4f}, Valid: {is_valid}"
    )

    if not is_valid:
        raise Exception(
            f"Cross-layer validation FAILED: dedup ratio {dedup_ratio:.4f} "
            f"is outside acceptable range [0.70, 1.00]. "
            f"Bronze={bronze_count}, Silver={silver_count}"
        )

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "dedup_ratio": dedup_ratio,
    }


@op(
    description="Validate that aggregated volumes in Gold match Silver sums",
    required_resource_keys={"iceberg_catalog"},
)
def validate_silver_gold_volume(context: OpExecutionContext):
    """
    Cross-layer check: Sum of daily_volume in Gold should equal the sum
    of quantities in Silver for the same date range.
    """
    catalog = context.resources.iceberg_catalog

    silver_df = catalog.load_table("silver.trades").scan().to_pandas()
    gold_df = catalog.load_table("gold.daily_summary").scan().to_pandas()

    # Group silver by (symbol, date) and sum quantities
    silver_df["trade_date"] = pd.to_datetime(silver_df["timestamp"]).dt.date
    silver_volumes = (
        silver_df.groupby(["symbol", "trade_date"])["quantity"]
        .sum()
        .reset_index()
        .rename(columns={"quantity": "silver_volume"})
    )

    # Merge and compare
    merged = gold_df.merge(
        silver_volumes,
        on=["symbol", "trade_date"],
        how="outer",
        indicator=True,
    )

    mismatches = merged[
        (merged["_merge"] != "both") |
        (merged["daily_volume"] != merged["silver_volume"])
    ]

    if len(mismatches) > 0:
        context.log.warning(
            f"Volume mismatches found: {len(mismatches)} rows. "
            f"Sample: {mismatches.head(5).to_dict()}"
        )
        raise Exception(
            f"Cross-layer validation FAILED: {len(mismatches)} volume mismatches "
            f"between Silver and Gold."
        )

    context.log.info("Silver-Gold volume validation PASSED.")
    return {"mismatches": 0}


@op(
    description="Validate symbol coverage consistency across layers",
    required_resource_keys={"iceberg_catalog"},
)
def validate_symbol_coverage(context: OpExecutionContext):
    """
    Cross-layer check: All symbols in Silver should appear in Gold,
    and vice versa (for the same date range).
    """
    catalog = context.resources.iceberg_catalog

    silver_symbols = set(
        catalog.load_table("silver.trades")
        .scan(selected_fields=("symbol",))
        .to_pandas()["symbol"]
        .unique()
    )

    gold_symbols = set(
        catalog.load_table("gold.daily_summary")
        .scan(selected_fields=("symbol",))
        .to_pandas()["symbol"]
        .unique()
    )

    missing_in_gold = silver_symbols - gold_symbols
    extra_in_gold = gold_symbols - silver_symbols

    if missing_in_gold:
        context.log.warning(f"Symbols in Silver but missing in Gold: {missing_in_gold}")

    if extra_in_gold:
        context.log.warning(f"Symbols in Gold but missing in Silver: {extra_in_gold}")

    is_valid = len(missing_in_gold) == 0 and len(extra_in_gold) == 0

    if not is_valid:
        raise Exception(
            f"Symbol coverage mismatch. Missing in Gold: {missing_in_gold}, "
            f"Extra in Gold: {extra_in_gold}"
        )

    context.log.info(
        f"Symbol coverage consistent: {len(silver_symbols)} symbols across layers."
    )
    return {"symbol_count": len(silver_symbols)}
```

Cross-layer validation job:

```python
# services/dagster-orchestrator/dagster_orchestrator/jobs/cross_layer_validation_job.py

from dagster import job

@job(
    resource_defs={"iceberg_catalog": iceberg_catalog_resource},
    description="Cross-layer data quality validation between Bronze, Silver, and Gold",
)
def cross_layer_validation_job():
    validate_bronze_silver_trade_count()
    validate_silver_gold_volume()
    validate_symbol_coverage()
```

---

## 4. Timeliness (Freshness SLAs)

### 4.1 Dagster FreshnessPolicy on Assets

Dagster's `FreshnessPolicy` allows you to define how stale an asset is allowed to be. The Dagster UI will display a "freshness" indicator and can trigger alerts when SLAs are breached.

```python
# services/dagster-orchestrator/dagster_orchestrator/assets/trade_assets.py

from dagster import asset, FreshnessPolicy, AutoMaterializePolicy
from datetime import timedelta


@asset(
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=1,
        cron_schedule=None,  # Continuous -- always enforce 1-minute lag
    ),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Raw trade data from Kafka ingestion",
)
def bronze_trades(context):
    """
    Bronze layer: Raw trade data.
    SLA: Maximum 1 minute lag from source.
    """
    # ... materialization logic
    pass


@asset(
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=5,
        cron_schedule=None,  # Continuous -- always enforce 5-minute lag
    ),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    description="Cleaned and deduplicated trade data",
)
def silver_trades(context, bronze_trades):
    """
    Silver layer: Cleaned trades.
    SLA: Maximum 5 minutes lag from Bronze.
    """
    # ... materialization logic
    pass


@asset(
    freshness_policy=FreshnessPolicy(
        maximum_lag_minutes=60,
        cron_schedule="30 16 * * 1-5",  # 4:30 PM ET, weekdays (after market close)
    ),
    description="Daily aggregated trade summaries",
)
def gold_daily_summary(context, silver_trades):
    """
    Gold layer: Daily OHLCV summaries.
    SLA: Maximum 1 hour lag after market close.
    """
    # ... materialization logic
    pass
```

**SLA Table**:

| Layer | Asset | Maximum Lag | Schedule | Rationale |
|---|---|---|---|---|
| Bronze | `bronze_trades` | 1 minute | Continuous | Near-real-time ingestion from Kafka |
| Silver | `silver_trades` | 5 minutes | Continuous | Processing and dedup overhead |
| Gold | `gold_daily_summary` | 1 hour | After market close (4:30 PM ET) | Daily aggregation batch job |

### 4.2 Freshness Sensor in Dagster

A dedicated sensor monitors asset freshness and emits alerts when SLAs are breached:

```python
# services/dagster-orchestrator/dagster_orchestrator/sensors/freshness_sensor.py

from dagster import (
    sensor,
    SensorEvaluationContext,
    AssetKey,
    RunRequest,
    SkipReason,
)
from datetime import datetime, timedelta
import requests

FRESHNESS_SLAS = {
    "bronze_trades": timedelta(minutes=1),
    "silver_trades": timedelta(minutes=5),
    "gold_daily_summary": timedelta(hours=1),
}

SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"


@sensor(
    minimum_interval_seconds=30,
    description="Monitor asset freshness and alert on SLA breaches",
)
def freshness_sla_sensor(context: SensorEvaluationContext):
    """Check freshness of all tracked assets and alert on SLA breaches."""
    breaches = []

    for asset_name, max_lag in FRESHNESS_SLAS.items():
        asset_key = AssetKey(asset_name)

        # Get latest materialization event
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=1,
        )

        if not records:
            breaches.append(f"{asset_name}: NEVER MATERIALIZED")
            continue

        last_materialized = datetime.fromtimestamp(records[0].timestamp)
        lag = datetime.now() - last_materialized

        if lag > max_lag:
            breaches.append(
                f"{asset_name}: lag={lag} exceeds SLA={max_lag} "
                f"(last materialized: {last_materialized})"
            )

    if breaches:
        alert_message = (
            f":rotating_light: *Freshness SLA Breach Detected*\n"
            f"Time: {datetime.now().isoformat()}\n\n"
            + "\n".join(f"- {b}" for b in breaches)
        )
        context.log.warning(alert_message)

        # Send Slack alert
        requests.post(SLACK_WEBHOOK_URL, json={"text": alert_message})

        return SkipReason(f"SLA breaches detected and alerted: {len(breaches)}")

    return SkipReason("All assets within freshness SLA.")
```

### 4.3 Alert on SLA Breach

SLA breach alerts are routed through the unified alerting system (Section 7). The freshness sensor triggers:

- **Slack notification**: Immediate alert with asset name, actual lag, and SLA threshold.
- **Dagster run failure**: If a critical asset (Bronze) is stale, the sensor can trigger a remediation job.
- **Metric emission**: Freshness metrics are pushed to Prometheus for Grafana dashboards (see observability plan).

---

## 5. Deduplication Strategy

### 5.1 Strategy by Layer

| Layer | Strategy | Rationale |
|---|---|---|
| **Bronze** | Accept duplicates (append-only) | Preserves raw data fidelity; duplicates are expected from Kafka at-least-once delivery |
| **Silver** | Dedup by `trade_id` using window functions | Silver is the "source of truth"; exactly-once semantics required |
| **Gold** | Natural dedup via aggregation | Aggregation (GROUP BY) inherently deduplicates |

### 5.2 Silver Layer Deduplication Implementation

**Approach 1: Window Function (Batch Processing)**

Used during batch Silver materialization to select the most recent version of each trade:

```sql
-- Deduplication query for Silver layer
-- Applied during Bronze -> Silver transformation

WITH ranked_trades AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_id
            ORDER BY ingestion_timestamp DESC
        ) AS row_num
    FROM bronze.trades
    WHERE ingestion_timestamp > :last_processed_timestamp
)
SELECT
    trade_id,
    symbol,
    timestamp,
    price,
    quantity,
    trade_type,
    status,
    exchange,
    bid_price,
    ask_price,
    ingestion_timestamp
FROM ranked_trades
WHERE row_num = 1
```

Equivalent in Pandas (used in Dagster ops):

```python
# services/dagster-orchestrator/dagster_orchestrator/ops/silver_transform.py

from dagster import op
import pandas as pd


@op(description="Deduplicate bronze trades for Silver layer")
def dedup_bronze_to_silver(context, bronze_df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate trades by trade_id, keeping the row with the latest
    ingestion_timestamp.
    """
    initial_count = len(bronze_df)

    # Sort by ingestion_timestamp descending, then drop duplicates keeping first
    deduped_df = (
        bronze_df
        .sort_values("ingestion_timestamp", ascending=False)
        .drop_duplicates(subset=["trade_id"], keep="first")
        .reset_index(drop=True)
    )

    dedup_count = initial_count - len(deduped_df)
    context.log.info(
        f"Dedup: {initial_count} rows -> {len(deduped_df)} rows "
        f"({dedup_count} duplicates removed, "
        f"{dedup_count / initial_count * 100:.2f}% dedup rate)"
    )

    return deduped_df
```

**Approach 2: Iceberg MERGE INTO (Upsert)**

Used for incremental Silver updates, leveraging Iceberg's MERGE INTO for efficient upserts:

```sql
-- Iceberg MERGE INTO for incremental Silver updates
-- This handles both new inserts and updates to existing trades

MERGE INTO silver.trades AS target
USING (
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY trade_id
                ORDER BY ingestion_timestamp DESC
            ) AS rn
        FROM bronze.trades
        WHERE ingestion_timestamp > :last_merge_timestamp
    )
    SELECT * FROM ranked WHERE rn = 1
) AS source
ON target.trade_id = source.trade_id
WHEN MATCHED AND source.ingestion_timestamp > target.ingestion_timestamp THEN
    UPDATE SET
        symbol = source.symbol,
        timestamp = source.timestamp,
        price = source.price,
        quantity = source.quantity,
        trade_type = source.trade_type,
        status = source.status,
        exchange = source.exchange,
        bid_price = source.bid_price,
        ask_price = source.ask_price,
        ingestion_timestamp = source.ingestion_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        trade_id, symbol, timestamp, price, quantity,
        trade_type, status, exchange, bid_price, ask_price,
        ingestion_timestamp
    )
    VALUES (
        source.trade_id, source.symbol, source.timestamp, source.price,
        source.quantity, source.trade_type, source.status, source.exchange,
        source.bid_price, source.ask_price, source.ingestion_timestamp
    )
```

### 5.3 Gold Layer Natural Deduplication

Gold aggregation inherently deduplicates because GROUP BY collapses rows:

```sql
-- Gold daily summary aggregation (natural dedup)
SELECT
    symbol,
    DATE(timestamp) AS trade_date,
    FIRST_VALUE(price) OVER (
        PARTITION BY symbol, DATE(timestamp)
        ORDER BY timestamp ASC
    ) AS daily_open,
    MAX(price) AS daily_high,
    MIN(price) AS daily_low,
    LAST_VALUE(price) OVER (
        PARTITION BY symbol, DATE(timestamp)
        ORDER BY timestamp ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS daily_close,
    SUM(quantity) AS daily_volume,
    COUNT(*) AS trade_count,
    SUM(price * quantity) / SUM(quantity) AS vwap
FROM silver.trades
GROUP BY symbol, DATE(timestamp)
```

---

## 6. Data Quality Scoring

### 6.1 Quality Score Calculation

Each validation batch produces a composite quality score across four dimensions:

```python
# services/data-quality/quality_scoring.py

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime


@dataclass
class QualityScore:
    """Composite quality score for a data batch."""

    batch_id: str
    layer: str  # bronze, silver, gold
    run_timestamp: datetime

    # Dimension scores (0.0 to 1.0)
    completeness_score: float = 0.0
    accuracy_score: float = 0.0
    freshness_score: float = 0.0
    uniqueness_score: float = 0.0

    # Weights for overall score calculation
    weights: Dict[str, float] = field(default_factory=lambda: {
        "completeness": 0.30,
        "accuracy": 0.30,
        "freshness": 0.20,
        "uniqueness": 0.20,
    })

    # Raw metrics
    total_rows: int = 0
    null_count: int = 0
    invalid_rows: int = 0
    duplicate_rows: int = 0
    within_sla: bool = True

    @property
    def overall_score(self) -> float:
        """Weighted average of all dimension scores."""
        return (
            self.weights["completeness"] * self.completeness_score
            + self.weights["accuracy"] * self.accuracy_score
            + self.weights["freshness"] * self.freshness_score
            + self.weights["uniqueness"] * self.uniqueness_score
        )

    @property
    def grade(self) -> str:
        """Letter grade based on overall score."""
        score = self.overall_score
        if score >= 0.95:
            return "A"
        elif score >= 0.85:
            return "B"
        elif score >= 0.70:
            return "C"
        elif score >= 0.50:
            return "D"
        else:
            return "F"

    def to_dict(self) -> Dict:
        return {
            "batch_id": self.batch_id,
            "layer": self.layer,
            "run_timestamp": self.run_timestamp.isoformat(),
            "completeness_score": round(self.completeness_score, 4),
            "accuracy_score": round(self.accuracy_score, 4),
            "freshness_score": round(self.freshness_score, 4),
            "uniqueness_score": round(self.uniqueness_score, 4),
            "overall_score": round(self.overall_score, 4),
            "grade": self.grade,
            "total_rows": self.total_rows,
            "null_count": self.null_count,
            "invalid_rows": self.invalid_rows,
            "duplicate_rows": self.duplicate_rows,
            "within_sla": self.within_sla,
        }


def compute_quality_score(
    batch_id: str,
    layer: str,
    total_rows: int,
    null_count: int,
    invalid_rows: int,
    duplicate_rows: int,
    within_sla: bool,
) -> QualityScore:
    """
    Compute quality scores from raw validation metrics.

    Formulas:
        completeness_score = 1 - (null_count / total_count)
        accuracy_score     = valid_rows / total_rows
        freshness_score    = 1 if within SLA, 0 otherwise
        uniqueness_score   = 1 - (duplicate_rows / total_rows)
        overall_score      = weighted average of above
    """
    score = QualityScore(
        batch_id=batch_id,
        layer=layer,
        run_timestamp=datetime.utcnow(),
        total_rows=total_rows,
        null_count=null_count,
        invalid_rows=invalid_rows,
        duplicate_rows=duplicate_rows,
        within_sla=within_sla,
    )

    if total_rows > 0:
        score.completeness_score = 1.0 - (null_count / total_rows)
        score.accuracy_score = (total_rows - invalid_rows) / total_rows
        score.uniqueness_score = 1.0 - (duplicate_rows / total_rows)
    else:
        score.completeness_score = 0.0
        score.accuracy_score = 0.0
        score.uniqueness_score = 0.0

    score.freshness_score = 1.0 if within_sla else 0.0

    return score
```

### 6.2 Quality Metrics Storage

Quality scores are persisted in a dedicated Iceberg table for historical trend analysis:

```sql
-- Iceberg table for quality validation results
CREATE TABLE quality.validation_results (
    batch_id            STRING      NOT NULL,
    layer               STRING      NOT NULL,   -- bronze, silver, gold
    run_timestamp       TIMESTAMP   NOT NULL,
    completeness_score  DOUBLE      NOT NULL,
    accuracy_score      DOUBLE      NOT NULL,
    freshness_score     DOUBLE      NOT NULL,
    uniqueness_score    DOUBLE      NOT NULL,
    overall_score       DOUBLE      NOT NULL,
    grade               STRING      NOT NULL,   -- A, B, C, D, F
    total_rows          BIGINT      NOT NULL,
    null_count          BIGINT      NOT NULL,
    invalid_rows        BIGINT      NOT NULL,
    duplicate_rows      BIGINT      NOT NULL,
    within_sla          BOOLEAN     NOT NULL,
    expectation_results STRING      NOT NULL,   -- JSON blob of per-expectation results
    metadata            STRING                  -- Additional context (JSON)
)
USING iceberg
PARTITIONED BY (layer, days(run_timestamp))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.metadata.compression-codec' = 'gzip'
);
```

Dagster op to persist quality scores:

```python
# services/dagster-orchestrator/dagster_orchestrator/ops/quality_scoring_ops.py

from dagster import op, Out
import pandas as pd
from quality_scoring import compute_quality_score


@op(
    description="Compute and store quality score for a validation run",
    required_resource_keys={"iceberg_catalog"},
)
def persist_quality_score(context, validation_result: dict):
    """
    Compute quality score from GE validation results and write to
    quality.validation_results Iceberg table.
    """
    score = compute_quality_score(
        batch_id=validation_result["batch_id"],
        layer=validation_result["layer"],
        total_rows=validation_result["total_rows"],
        null_count=validation_result["null_count"],
        invalid_rows=validation_result["invalid_rows"],
        duplicate_rows=validation_result["duplicate_rows"],
        within_sla=validation_result["within_sla"],
    )

    score_df = pd.DataFrame([score.to_dict()])

    # Append to Iceberg table
    catalog = context.resources.iceberg_catalog
    table = catalog.load_table("quality.validation_results")
    table.append(score_df)

    context.log.info(
        f"Quality score persisted: layer={score.layer}, "
        f"overall={score.overall_score:.4f}, grade={score.grade}"
    )

    return score.to_dict()
```

### 6.3 Historical Quality Trend Tracking

Query for quality trend analysis:

```sql
-- Quality trend over the last 30 days by layer
SELECT
    layer,
    DATE(run_timestamp) AS run_date,
    AVG(overall_score) AS avg_quality_score,
    MIN(overall_score) AS min_quality_score,
    MAX(overall_score) AS max_quality_score,
    COUNT(*) AS validation_runs,
    SUM(CASE WHEN grade IN ('D', 'F') THEN 1 ELSE 0 END) AS failed_runs
FROM quality.validation_results
WHERE run_timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY layer, DATE(run_timestamp)
ORDER BY layer, run_date;

-- Worst-performing expectations (most frequent failures)
SELECT
    layer,
    json_extract(expectation_results, '$.expectation_type') AS expectation_type,
    COUNT(*) AS failure_count
FROM quality.validation_results
WHERE grade IN ('D', 'F')
  AND run_timestamp >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY layer, expectation_type
ORDER BY failure_count DESC
LIMIT 20;
```

---

## 7. Alerting on Quality Failures

### 7.1 Dagster Failure Hooks

Dagster hooks intercept op/asset failures and route alerts to the appropriate channels:

```python
# services/dagster-orchestrator/dagster_orchestrator/hooks/quality_hooks.py

from dagster import (
    HookContext,
    failure_hook,
    success_hook,
)
import requests
import json
from datetime import datetime
from enum import Enum


class Severity(Enum):
    WARNING = "warning"     # Soft failure: log and alert, continue processing
    CRITICAL = "critical"   # Hard failure: alert and stop downstream processing


SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
EMAIL_ALERT_ENDPOINT = "http://internal-alert-service/api/v1/email"


def send_slack_alert(severity: Severity, message: str):
    """Send alert to Slack via webhook."""
    emoji = ":warning:" if severity == Severity.WARNING else ":rotating_light:"
    color = "#FFA500" if severity == Severity.WARNING else "#FF0000"

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emoji} Data Quality {severity.value.upper()} {emoji}",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message,
                        },
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"Timestamp: {datetime.now().isoformat()}",
                            }
                        ],
                    },
                ],
            }
        ]
    }

    requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)


def send_email_alert(severity: Severity, subject: str, body: str):
    """Send alert via email."""
    payload = {
        "to": ["data-team@example.com"],
        "subject": f"[{severity.value.upper()}] {subject}",
        "body": body,
    }
    if severity == Severity.CRITICAL:
        payload["to"].append("oncall@example.com")

    requests.post(EMAIL_ALERT_ENDPOINT, json=payload, timeout=10)


@failure_hook
def quality_failure_hook(context: HookContext):
    """
    Hook that fires on any quality validation failure.
    Determines severity from op metadata and routes alerts.
    """
    op_name = context.op.name
    error = context.op_exception

    # Determine severity from op tags or default to CRITICAL
    severity_tag = context.op.tags.get("quality_severity", "critical")
    severity = Severity(severity_tag)

    message = (
        f"*Op*: `{op_name}`\n"
        f"*Severity*: {severity.value.upper()}\n"
        f"*Run ID*: {context.run_id}\n"
        f"*Error*: {str(error)[:500]}"
    )

    # Always send Slack alert
    send_slack_alert(severity, message)

    # Send email for CRITICAL failures
    if severity == Severity.CRITICAL:
        send_email_alert(
            severity,
            subject=f"Data Quality CRITICAL: {op_name}",
            body=f"Op {op_name} failed with error:\n\n{str(error)}",
        )

    context.log.error(f"Quality failure alert sent: severity={severity.value}")


@success_hook
def quality_success_hook(context: HookContext):
    """Log successful quality validations for observability."""
    context.log.info(f"Quality validation passed: {context.op.name}")
```

### 7.2 Severity Levels

| Severity | Meaning | Actions | Example |
|---|---|---|---|
| **WARNING** | Soft failure. Data quality is degraded but not critically broken. | Log, Slack alert, continue downstream processing. | Distribution check fails (slightly anomalous mean) |
| **CRITICAL** | Hard failure. Data integrity is compromised. | Log, Slack alert, email alert, **stop downstream processing** (circuit breaker). | Null trade_id detected, schema mismatch, dedup ratio out of bounds |

### 7.3 Circuit Breaker: Stop Downstream Processing on CRITICAL

When a CRITICAL quality failure occurs, downstream assets must not materialize. This is implemented through Dagster's asset check blocking:

```python
# services/dagster-orchestrator/dagster_orchestrator/assets/trade_assets.py

from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetIn,
    AutoMaterializePolicy,
)


@asset(
    check_specs=[
        AssetCheckSpec(
            name="bronze_quality_gate",
            asset="bronze_trades",
            description="Quality gate: blocks Silver if Bronze fails validation",
            blocking=True,  # CRITICAL: blocks downstream materialization
        )
    ],
)
def bronze_trades(context):
    # ... materialization logic
    pass


@asset_check(asset="bronze_trades")
def bronze_quality_gate(context, bronze_trades) -> AssetCheckResult:
    """Circuit breaker: if Bronze quality fails, Silver will not materialize."""
    # Run GE validation
    # ... (abbreviated -- full implementation in Section 2.3)

    if not validation_passed:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"reason": "Bronze quality gate FAILED"},
        )

    return AssetCheckResult(passed=True)
```

With `blocking=True`, Dagster will refuse to materialize `silver_trades` (which depends on `bronze_trades`) if the `bronze_quality_gate` check fails. This is the circuit breaker mechanism.

---

## 8. Data Docs (Auto-Generated Documentation)

### 8.1 Great Expectations Data Docs Setup

Data Docs are configured in the `great_expectations.yml` (see Section 1.3). They are automatically generated after each checkpoint run.

```yaml
# Checkpoint configuration with Data Docs update action
# services/data-quality/great_expectations/checkpoints/bronze_checkpoint.yml

name: bronze_checkpoint
config_version: 1.0
class_name: Checkpoint
run_name_template: "bronze_validation_%Y%m%d_%H%M%S"
validations:
  - batch_request:
      datasource_name: iceberg_bronze
      data_connector_name: bronze_connector
      data_asset_name: trades
    expectation_suite_name: bronze_trades_suite
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names:
        - local_site
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
```

### 8.2 Serve via Static Site in Dagster

Data Docs are served as a static site accessible from the Dagster UI:

```python
# services/dagster-orchestrator/dagster_orchestrator/resources/data_docs_server.py

from dagster import resource
import http.server
import threading
import os

DATA_DOCS_DIR = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "data-quality", "great_expectations",
    "uncommitted", "data_docs", "local_site",
)


@resource(description="Serves Great Expectations Data Docs as a static site")
def data_docs_server_resource(context):
    """Start a simple HTTP server for Data Docs on port 8765."""
    handler = http.server.SimpleHTTPRequestHandler

    os.chdir(DATA_DOCS_DIR)
    server = http.server.HTTPServer(("0.0.0.0", 8765), handler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    context.log.info(f"Data Docs server started at http://localhost:8765")

    return server
```

Alternatively, configure a Dagster link to the Data Docs:

```python
# services/dagster-orchestrator/dagster_orchestrator/definitions.py

from dagster import Definitions, ExternalAssetDependency

defs = Definitions(
    assets=[bronze_trades, silver_trades, gold_daily_summary],
    asset_checks=[
        bronze_trades_quality_check,
        silver_trades_quality_check,
        gold_daily_summary_quality_check,
    ],
    resources={
        "ge_data_context": ge_data_context_resource,
        "iceberg_catalog": iceberg_catalog_resource,
    },
    jobs=[full_quality_validation_job, cross_layer_validation_job],
    sensors=[cross_layer_quality_sensor, freshness_sla_sensor],
)
```

### 8.3 Validation Result History

Data Docs automatically maintain a history of all validation runs. Each run is timestamped and shows:

- **Suite-level results**: Overall pass/fail, run time, row count.
- **Expectation-level results**: Each expectation's pass/fail, observed values, element counts.
- **Unexpected values**: Samples of rows that failed expectations (configurable via `result_format`).

### 8.4 Expectation Suite Documentation

Data Docs generate human-readable documentation for each expectation suite, including:

- Suite name and description.
- Each expectation's type, parameters, and meta tags.
- Historical pass rates per expectation.
- Links to the most recent validation run.

This documentation is valuable for data consumers who need to understand what quality guarantees each layer provides.

---

## 9. Implementation Steps

### 9.1 File Paths and Creation Order

All files are created under the monorepo root. Dependencies are listed in creation order.

**Phase 1: Foundation (Week 1)**

| Step | File Path | Description |
|---|---|---|
| 1 | `services/data-quality/pyproject.toml` | Python project configuration with GE dependencies |
| 2 | `services/data-quality/requirements.txt` | Pinned dependency versions |
| 3 | `services/data-quality/great_expectations/great_expectations.yml` | GE Data Context configuration |
| 4 | `services/data-quality/great_expectations/plugins/__init__.py` | Custom expectations package init |
| 5 | `services/data-quality/great_expectations/plugins/custom_expectations/__init__.py` | Custom expectations module init |
| 6 | `services/data-quality/great_expectations/.gitignore` | Ignore uncommitted/data_docs output |

**Phase 2: Expectation Suites (Week 1-2)**

| Step | File Path | Description |
|---|---|---|
| 7 | `services/data-quality/great_expectations/expectations/bronze_trades_suite.json` | Bronze layer expectation suite |
| 8 | `services/data-quality/great_expectations/expectations/silver_trades_suite.json` | Silver layer expectation suite |
| 9 | `services/data-quality/great_expectations/expectations/gold_daily_summary_suite.json` | Gold layer expectation suite |
| 10 | `services/data-quality/great_expectations/expectations/cross_layer_suite.json` | Cross-source validation suite |

**Phase 3: Custom Expectations (Week 2)**

| Step | File Path | Description |
|---|---|---|
| 11 | `services/data-quality/great_expectations/plugins/custom_expectations/expect_vwap_between_high_and_low.py` | Custom: VWAP in [low, high] range |
| 12 | `services/data-quality/great_expectations/plugins/custom_expectations/expect_high_gte_open_and_close.py` | Custom: High >= Open and Close |
| 13 | `services/data-quality/great_expectations/plugins/custom_expectations/expect_trade_count_matches_source.py` | Custom: Cross-layer trade count match |

**Phase 4: Checkpoints (Week 2)**

| Step | File Path | Description |
|---|---|---|
| 14 | `services/data-quality/great_expectations/checkpoints/bronze_checkpoint.yml` | Bronze validation checkpoint |
| 15 | `services/data-quality/great_expectations/checkpoints/silver_checkpoint.yml` | Silver validation checkpoint |
| 16 | `services/data-quality/great_expectations/checkpoints/gold_checkpoint.yml` | Gold validation checkpoint |

**Phase 5: Dagster Integration (Week 2-3)**

| Step | File Path | Description |
|---|---|---|
| 17 | `services/dagster-orchestrator/dagster_orchestrator/resources/ge_resource.py` | GE Data Context Dagster resource |
| 18 | `services/dagster-orchestrator/dagster_orchestrator/ops/quality_ops.py` | GE validation ops via ge_validation_op_factory |
| 19 | `services/dagster-orchestrator/dagster_orchestrator/ops/cross_layer_validation.py` | Cross-layer validation ops |
| 20 | `services/dagster-orchestrator/dagster_orchestrator/ops/quality_scoring_ops.py` | Quality score computation and persistence ops |
| 21 | `services/dagster-orchestrator/dagster_orchestrator/assets/quality_checks.py` | Asset checks for inline quality validation |
| 22 | `services/dagster-orchestrator/dagster_orchestrator/jobs/quality_jobs.py` | Full quality validation job |
| 23 | `services/dagster-orchestrator/dagster_orchestrator/jobs/cross_layer_validation_job.py` | Cross-layer validation job |

**Phase 6: Sensors and Hooks (Week 3)**

| Step | File Path | Description |
|---|---|---|
| 24 | `services/dagster-orchestrator/dagster_orchestrator/sensors/quality_sensors.py` | Sensor to trigger cross-layer validation |
| 25 | `services/dagster-orchestrator/dagster_orchestrator/sensors/freshness_sensor.py` | Freshness SLA monitoring sensor |
| 26 | `services/dagster-orchestrator/dagster_orchestrator/hooks/quality_hooks.py` | Failure/success hooks for alerting |

**Phase 7: Quality Scoring (Week 3)**

| Step | File Path | Description |
|---|---|---|
| 27 | `services/data-quality/quality_scoring.py` | Quality score computation module |
| 28 | `services/data-quality/schemas/validation_results.sql` | Iceberg table DDL for quality metrics |

**Phase 8: Testing (Week 3-4)**

| Step | File Path | Description |
|---|---|---|
| 29 | `services/data-quality/tests/__init__.py` | Test package init |
| 30 | `services/data-quality/tests/fixtures/valid_trades.csv` | Valid trade data fixture |
| 31 | `services/data-quality/tests/fixtures/invalid_trades_nulls.csv` | Trades with null values |
| 32 | `services/data-quality/tests/fixtures/invalid_trades_types.csv` | Trades with type mismatches |
| 33 | `services/data-quality/tests/fixtures/duplicate_trades.csv` | Trades with duplicates |
| 34 | `services/data-quality/tests/test_bronze_expectations.py` | Bronze suite unit tests |
| 35 | `services/data-quality/tests/test_silver_expectations.py` | Silver suite unit tests |
| 36 | `services/data-quality/tests/test_gold_expectations.py` | Gold suite unit tests |

### 9.2 Dependency Graph

```
Phase 1 (Foundation)
    |
    v
Phase 2 (Expectation Suites) -----> Phase 3 (Custom Expectations)
    |                                       |
    v                                       v
Phase 4 (Checkpoints) <---------------------
    |
    v
Phase 5 (Dagster Integration)
    |
    +---> Phase 6 (Sensors & Hooks)
    |
    +---> Phase 7 (Quality Scoring)
    |
    v
Phase 8 (Testing) -- depends on all above
```

---

## 10. Testing Strategy

### 10.1 Unit Tests for Custom Expectations

Each custom expectation is tested independently with small, focused DataFrames:

```python
# services/data-quality/tests/test_gold_expectations.py

import pytest
import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationSuiteValidationResult


@pytest.fixture
def ge_context():
    """Create a fresh GE context for testing."""
    return gx.get_context(
        context_root_dir="services/data-quality/great_expectations"
    )


@pytest.fixture
def valid_gold_df():
    """Gold daily summary with valid data."""
    return pd.DataFrame({
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "trade_date": ["2025-01-15", "2025-01-15", "2025-01-15"],
        "daily_open": [150.00, 140.00, 400.00],
        "daily_high": [155.00, 145.00, 410.00],
        "daily_low": [148.00, 138.00, 395.00],
        "daily_close": [153.00, 143.00, 405.00],
        "daily_volume": [1000000, 500000, 750000],
        "trade_count": [5000, 3000, 4000],
        "vwap": [152.50, 142.00, 403.00],
    })


@pytest.fixture
def invalid_gold_vwap_df():
    """Gold daily summary where VWAP is outside [low, high] range."""
    return pd.DataFrame({
        "symbol": ["AAPL"],
        "trade_date": ["2025-01-15"],
        "daily_open": [150.00],
        "daily_high": [155.00],
        "daily_low": [148.00],
        "daily_close": [153.00],
        "daily_volume": [1000000],
        "trade_count": [5000],
        "vwap": [160.00],  # INVALID: VWAP > daily_high
    })


class TestGoldDailySummarySuite:
    """Tests for gold_daily_summary_suite expectations."""

    def test_valid_data_passes_all_expectations(self, ge_context, valid_gold_df):
        """Valid gold summary data should pass all expectations."""
        result = ge_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            batch_request=RuntimeBatchRequest(
                datasource_name="iceberg_gold",
                data_connector_name="gold_connector",
                data_asset_name="test_data",
                runtime_parameters={"batch_data": valid_gold_df},
                batch_identifiers={"batch_id": "test_valid"},
            ),
        )
        assert result.success is True

    def test_vwap_outside_range_fails(self, ge_context, invalid_gold_vwap_df):
        """VWAP outside [daily_low, daily_high] should fail."""
        result = ge_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            batch_request=RuntimeBatchRequest(
                datasource_name="iceberg_gold",
                data_connector_name="gold_connector",
                data_asset_name="test_data",
                runtime_parameters={"batch_data": invalid_gold_vwap_df},
                batch_identifiers={"batch_id": "test_invalid_vwap"},
            ),
        )
        assert result.success is False

        # Find the specific failed expectation
        failed = [r for r in result.results if not r.success]
        failed_types = [r.expectation_config.expectation_type for r in failed]
        assert "expect_column_pair_values_A_to_be_greater_than_B" in failed_types

    def test_negative_volume_fails(self, ge_context):
        """Negative daily_volume should fail the business rule check."""
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2025-01-15"],
            "daily_open": [150.00],
            "daily_high": [155.00],
            "daily_low": [148.00],
            "daily_close": [153.00],
            "daily_volume": [-100],  # INVALID: negative volume
            "trade_count": [5000],
            "vwap": [152.50],
        })

        result = ge_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            batch_request=RuntimeBatchRequest(
                datasource_name="iceberg_gold",
                data_connector_name="gold_connector",
                data_asset_name="test_data",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": "test_neg_volume"},
            ),
        )
        assert result.success is False

    def test_high_less_than_open_fails(self, ge_context):
        """daily_high < daily_open should fail."""
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2025-01-15"],
            "daily_open": [155.00],
            "daily_high": [150.00],  # INVALID: high < open
            "daily_low": [148.00],
            "daily_close": [153.00],
            "daily_volume": [1000000],
            "trade_count": [5000],
            "vwap": [149.00],
        })

        result = ge_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            batch_request=RuntimeBatchRequest(
                datasource_name="iceberg_gold",
                data_connector_name="gold_connector",
                data_asset_name="test_data",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": "test_high_lt_open"},
            ),
        )
        assert result.success is False

    def test_null_aggregated_column_fails(self, ge_context):
        """Null values in aggregated columns should fail completeness checks."""
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2025-01-15"],
            "daily_open": [150.00],
            "daily_high": [None],  # INVALID: null
            "daily_low": [148.00],
            "daily_close": [153.00],
            "daily_volume": [1000000],
            "trade_count": [5000],
            "vwap": [152.50],
        })

        result = ge_context.run_checkpoint(
            checkpoint_name="gold_checkpoint",
            batch_request=RuntimeBatchRequest(
                datasource_name="iceberg_gold",
                data_connector_name="gold_connector",
                data_asset_name="test_data",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": "test_null_col"},
            ),
        )
        assert result.success is False
```

### 10.2 Test with Known Good/Bad Data Samples

**Test Fixtures**:

```csv
# services/data-quality/tests/fixtures/valid_trades.csv
trade_id,symbol,timestamp,price,quantity,trade_type,status,exchange,bid_price,ask_price,ingestion_timestamp
550e8400-e29b-41d4-a716-446655440001,AAPL,2025-01-15T10:30:00Z,150.25,100,BUY,FILLED,NASDAQ,150.20,150.30,2025-01-15T10:30:01Z
550e8400-e29b-41d4-a716-446655440002,GOOGL,2025-01-15T10:30:05Z,140.50,200,SELL,FILLED,NASDAQ,140.45,140.55,2025-01-15T10:30:06Z
550e8400-e29b-41d4-a716-446655440003,MSFT,2025-01-15T10:30:10Z,400.75,50,BUY,NEW,NYSE,400.70,400.80,2025-01-15T10:30:11Z
```

```csv
# services/data-quality/tests/fixtures/invalid_trades_nulls.csv
trade_id,symbol,timestamp,price,quantity,trade_type,status,exchange,bid_price,ask_price,ingestion_timestamp
,AAPL,2025-01-15T10:30:00Z,150.25,100,BUY,FILLED,NASDAQ,150.20,150.30,2025-01-15T10:30:01Z
550e8400-e29b-41d4-a716-446655440002,,2025-01-15T10:30:05Z,140.50,200,SELL,FILLED,NASDAQ,140.45,140.55,2025-01-15T10:30:06Z
550e8400-e29b-41d4-a716-446655440003,MSFT,,400.75,50,BUY,NEW,NYSE,400.70,400.80,2025-01-15T10:30:11Z
```

```csv
# services/data-quality/tests/fixtures/invalid_trades_types.csv
trade_id,symbol,timestamp,price,quantity,trade_type,status,exchange,bid_price,ask_price,ingestion_timestamp
550e8400-e29b-41d4-a716-446655440001,AAPL,2025-01-15T10:30:00Z,not_a_number,100,BUY,FILLED,NASDAQ,150.20,150.30,2025-01-15T10:30:01Z
550e8400-e29b-41d4-a716-446655440002,GOOGL,2025-01-15T10:30:05Z,140.50,not_an_int,SELL,FILLED,NASDAQ,140.45,140.55,2025-01-15T10:30:06Z
550e8400-e29b-41d4-a716-446655440003,MSFT,2025-01-15T10:30:10Z,400.75,50,HOLD,INVALID_STATUS,NYSE,400.70,400.80,2025-01-15T10:30:11Z
```

```csv
# services/data-quality/tests/fixtures/duplicate_trades.csv
trade_id,symbol,timestamp,price,quantity,trade_type,status,exchange,bid_price,ask_price,ingestion_timestamp
550e8400-e29b-41d4-a716-446655440001,AAPL,2025-01-15T10:30:00Z,150.25,100,BUY,FILLED,NASDAQ,150.20,150.30,2025-01-15T10:30:01Z
550e8400-e29b-41d4-a716-446655440001,AAPL,2025-01-15T10:30:00Z,150.25,100,BUY,FILLED,NASDAQ,150.20,150.30,2025-01-15T10:30:05Z
550e8400-e29b-41d4-a716-446655440001,AAPL,2025-01-15T10:30:00Z,150.30,100,BUY,FILLED,NASDAQ,150.25,150.35,2025-01-15T10:30:10Z
```

### 10.3 Test Matrix

| Test Category | Suite | Input | Expected Result |
|---|---|---|---|
| Valid data passes | Bronze | `valid_trades.csv` | All expectations pass |
| Null values detected | Bronze | `invalid_trades_nulls.csv` | Completeness checks fail |
| Type mismatches detected | Bronze | `invalid_trades_types.csv` | Schema checks fail |
| Invalid enums detected | Bronze | `invalid_trades_types.csv` | Validity checks fail (HOLD, INVALID_STATUS) |
| Duplicates detected | Silver | `duplicate_trades.csv` | Uniqueness checks fail |
| Price out of range | Silver | Custom fixture (price=-1) | Accuracy checks fail |
| Ask <= Bid detected | Silver | Custom fixture | Accuracy check fails |
| Unknown symbol detected | Silver | Custom fixture (symbol=ZZZZZ) | Referential integrity fails |
| Invalid UUID format | Silver | Custom fixture (trade_id=abc123) | Consistency check fails |
| VWAP out of range | Gold | Custom fixture | Business rule fails |
| High < Open | Gold | Custom fixture | Business rule fails |
| Negative volume | Gold | Custom fixture | Business rule fails |
| Cross-layer count mismatch | Cross | Simulated bronze/silver | Cross-source validation fails |
| Quality score computation | Scoring | Various metrics | Correct score calculation |
| Quality score persistence | Scoring | Valid score | Written to Iceberg table |

### 10.4 Running Tests

```bash
# Run all data quality tests
cd services/data-quality
pytest tests/ -v --tb=short

# Run specific suite tests
pytest tests/test_bronze_expectations.py -v
pytest tests/test_silver_expectations.py -v
pytest tests/test_gold_expectations.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run quality scoring unit tests
pytest tests/test_quality_scoring.py -v
```

---

## Appendix A: Configuration Reference

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `GE_ROOT_DIR` | Path to Great Expectations project root | `services/data-quality/great_expectations` |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL for alerts | (required) |
| `EMAIL_ALERT_ENDPOINT` | Internal email alert API endpoint | (required) |
| `ICEBERG_CATALOG_URI` | Iceberg REST catalog URI | `http://localhost:8181` |
| `DATA_DOCS_PORT` | Port for Data Docs HTTP server | `8765` |

### Dagster Resource Configuration

```python
# Example resource configuration for local development
resource_defs = {
    "ge_data_context": ge_data_context_resource.configured({
        "ge_root_dir": "services/data-quality/great_expectations",
    }),
    "iceberg_catalog": iceberg_catalog_resource.configured({
        "uri": "http://localhost:8181",
        "warehouse": "local_warehouse",
    }),
}
```

## Appendix B: Quality Dashboard Queries

These queries power the Grafana quality dashboard (see observability plan):

```sql
-- Current quality score by layer
SELECT layer, overall_score, grade, run_timestamp
FROM quality.validation_results
WHERE run_timestamp = (
    SELECT MAX(run_timestamp) FROM quality.validation_results AS sub
    WHERE sub.layer = quality.validation_results.layer
);

-- Quality score trend (7-day rolling average)
SELECT
    layer,
    DATE(run_timestamp) AS day,
    AVG(overall_score) OVER (
        PARTITION BY layer
        ORDER BY DATE(run_timestamp)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM quality.validation_results
ORDER BY layer, day;

-- SLA compliance rate (last 30 days)
SELECT
    layer,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) AS within_sla,
    ROUND(100.0 * SUM(CASE WHEN within_sla THEN 1 ELSE 0 END) / COUNT(*), 2) AS sla_compliance_pct
FROM quality.validation_results
WHERE run_timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY layer;
```
