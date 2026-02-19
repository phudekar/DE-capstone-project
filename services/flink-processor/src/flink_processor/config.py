"""Flink processor configuration: topics, bootstrap servers, thresholds."""

import json
import os
from pathlib import Path

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:29092")

# --- Input topics ---
TOPIC_RAW_TRADES = "raw.trades"
TOPIC_RAW_ORDERBOOK = "raw.orderbook-snapshots"

# --- Output topics ---
TOPIC_TRADE_AGGREGATES = "analytics.trade-aggregates"
TOPIC_ORDERBOOK_METRICS = "analytics.orderbook-metrics"
TOPIC_PRICE_ALERTS = "alerts.price-movement"
TOPIC_ENRICHED_TRADES = "enriched.trades"
TOPIC_DLQ = "dlq.processing-errors"

# --- Window sizes ---
WINDOW_SIZES = ["1m", "5m", "15m"]

# --- Price alert thresholds ---
ALERT_THRESHOLD_MEDIUM = 0.02   # 2%
ALERT_THRESHOLD_HIGH = 0.05     # 5%
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
