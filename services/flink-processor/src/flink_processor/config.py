"""Flink processor configuration: topics, bootstrap servers, thresholds.

Centralises all external configuration (Kafka broker addresses, topic names,
alert thresholds, Flink tuning knobs) so that the pipeline entry points
(submit_python_pipeline, submit_sql_pipeline) and operator modules import
constants from a single source of truth.

Topic naming convention follows a namespace prefix:
  - raw.*        — ingested events from the exchange simulator
  - analytics.*  — derived metrics produced by Flink jobs
  - alerts.*     — threshold-triggered notifications
  - enriched.*   — raw events augmented with reference/dimension data
  - dlq.*        — dead-letter queue for records that fail processing
"""

import json
import os
from pathlib import Path

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")

# --- Input topics ---
TOPIC_RAW_TRADES = "raw.trades"  # Trade executions from the exchange simulator
TOPIC_RAW_ORDERBOOK = "raw.orderbook-snapshots"  # Periodic L2 orderbook snapshots

# --- Output topics ---
TOPIC_TRADE_AGGREGATES = "analytics.trade-aggregates"  # Tumbling-window OHLCV aggregates (1m/5m/15m)
TOPIC_ORDERBOOK_METRICS = "analytics.orderbook-metrics"  # Spread, depth, imbalance per snapshot
TOPIC_PRICE_ALERTS = "alerts.price-movement"  # Triggered when price moves >2% from baseline
TOPIC_ENRICHED_TRADES = "enriched.trades"  # Trades joined with reference symbol metadata
TOPIC_DLQ = "dlq.processing-errors"  # Dead-letter queue for malformed/unparseable records

# --- Price alert thresholds ---
ALERT_THRESHOLD_MEDIUM = 0.02  # 2%
ALERT_THRESHOLD_HIGH = 0.05  # 5%
ALERT_THRESHOLD_CRITICAL = 0.10  # 10%
ALERT_COOLDOWN_SECONDS = 60
ALERT_BASELINE_WINDOW_SECONDS = 300  # 5 min

# --- Flink settings ---
CHECKPOINT_INTERVAL_MS = 60000
CHECKPOINT_MODE = "AT_LEAST_ONCE"
PARALLELISM = 1

# --- Reference data ---
_REFERENCE_DATA_PATH = os.getenv(
    "REFERENCE_DATA_PATH",
    str(Path(__file__).resolve().parent.parent.parent / "data" / "reference" / "symbols.json"),
)


def load_reference_symbols() -> dict[str, dict]:
    """Load symbol reference data keyed by ticker symbol."""
    with open(_REFERENCE_DATA_PATH) as f:
        symbols = json.load(f)
    return {s["symbol"]: s for s in symbols}
