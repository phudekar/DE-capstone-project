# 03 - Stream Processing & Compute: Implementation Plan

> **Component**: Apache Flink (stream processing) + DuckDB (ad-hoc analytics)
> **Data Domain**: Stock market trades, order books, market data
> **Upstream Dependency**: Kafka cluster with raw trade data ingested from WebSocket sources
> **Downstream Consumers**: Iceberg/Parquet storage layer, API serving layer, monitoring dashboards

---

## Table of Contents

1. [Apache Flink Setup](#1-apache-flink-setup)
2. [Flink Stream Processing Jobs](#2-flink-stream-processing-jobs)
3. [Kafka Topic Architecture](#3-kafka-topic-architecture)
4. [State Management](#4-state-management)
5. [DuckDB Integration](#5-duckdb-integration)
6. [Exactly-Once Semantics](#6-exactly-once-semantics)
7. [Error Handling](#7-error-handling)
8. [Implementation Steps](#8-implementation-steps)
9. [Testing Strategy](#9-testing-strategy)
10. [Monitoring](#10-monitoring)

---

## 1. Apache Flink Setup

### 1.1 Cluster Architecture

The Flink cluster runs in Session Mode inside Docker containers, managed alongside Kafka via Docker Compose. The cluster consists of a single JobManager (master) and two TaskManagers (workers) for local development. Production-like settings are achieved through resource limits and realistic parallelism.

```
+------------------+       +-------------------+       +-------------------+
|   JobManager     |       |  TaskManager #1   |       |  TaskManager #2   |
|  (Master Node)   |<----->|  (4 task slots)   |       |  (4 task slots)   |
|  REST port: 8081 |       |  Heap: 2GB        |       |  Heap: 2GB        |
|  RPC  port: 6123 |       |  Managed: 1GB     |       |  Managed: 1GB     |
+------------------+       +-------------------+       +-------------------+
         |
         v
   Web Dashboard
   http://localhost:8081
```

### 1.2 PyFlink vs Java Flink Decision

**Recommendation: PyFlink**

| Criteria                  | PyFlink                                    | Java Flink                              |
|---------------------------|--------------------------------------------|-----------------------------------------|
| Language consistency      | Matches Python monorepo                    | Requires separate build toolchain       |
| Development speed         | Faster iteration, no compile step          | Slower dev cycle                        |
| Ecosystem integration     | Native DuckDB, pandas, numpy access        | Requires JNI bridges                    |
| Table API / SQL support   | Full support via PyFlink Table API         | Full support                            |
| DataStream API            | Supported (Flink 1.16+)                    | Native                                  |
| Performance               | Slight overhead from Python UDFs           | Native JVM performance                  |
| CEP (Complex Event Proc.) | Available via Table API patterns           | Full CEP library                        |
| Community examples        | Growing but smaller                        | Extensive                               |

PyFlink is the right choice because this is a Python monorepo, the team works primarily in Python, and the DuckDB integration is a first-class Python library. The slight performance overhead of PyFlink UDFs is negligible for the expected trade volumes. For any performance-critical hot path, we can fall back to Flink SQL which runs natively on the JVM even when submitted from PyFlink.

### 1.3 Flink Configuration

File: `services/flink-processor/conf/flink-conf.yaml`

```yaml
# === JobManager Configuration ===
jobmanager.rpc.address: jobmanager
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 1600m
jobmanager.execution.failover-strategy: region

# === TaskManager Configuration ===
taskmanager.memory.process.size: 4096m
taskmanager.memory.managed.size: 1024m
taskmanager.memory.network.fraction: 0.1
taskmanager.memory.network.min: 64mb
taskmanager.memory.network.max: 256mb
taskmanager.numberOfTaskSlots: 4

# === Parallelism ===
parallelism.default: 4

# === Checkpointing ===
execution.checkpointing.interval: 30000          # 30 seconds
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.min-pause: 10000          # 10 seconds between checkpoints
execution.checkpointing.timeout: 120000           # 2 minute timeout
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.unaligned.enabled: true   # Reduce backpressure impact

# === State Backend ===
state.backend.type: rocksdb
state.backend.rocksdb.localdir: /tmp/flink-rocksdb
state.backend.incremental: true                   # Incremental checkpoints for large state
state.checkpoints.dir: file:///opt/flink/checkpoints
state.savepoints.dir: file:///opt/flink/savepoints

# === Restart Strategy ===
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s

# === Web Dashboard ===
rest.port: 8081
rest.address: 0.0.0.0
web.submit.enable: true

# === Metrics ===
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
```

### 1.4 Docker Compose Service Definitions

File: `infrastructure/docker/docker-compose.flink.yml`

```yaml
services:
  jobmanager:
    image: flink:1.18-java11
    container_name: flink-jobmanager
    ports:
      - "8081:8081"    # Web UI
      - "6123:6123"    # RPC
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        state.backend.type: rocksdb
        state.backend.incremental: true
        execution.checkpointing.interval: 30000
        execution.checkpointing.mode: EXACTLY_ONCE
        state.checkpoints.dir: file:///opt/flink/checkpoints
        state.savepoints.dir: file:///opt/flink/savepoints
    volumes:
      - flink-checkpoints:/opt/flink/checkpoints
      - flink-savepoints:/opt/flink/savepoints
      - ./services/flink-processor:/opt/flink/usrlib
    networks:
      - data-platform

  taskmanager-1:
    image: flink:1.18-java11
    container_name: flink-taskmanager-1
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 4096m
        taskmanager.memory.managed.size: 1024m
    volumes:
      - ./services/flink-processor:/opt/flink/usrlib
    networks:
      - data-platform

  taskmanager-2:
    image: flink:1.18-java11
    container_name: flink-taskmanager-2
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 4096m
        taskmanager.memory.managed.size: 1024m
    volumes:
      - ./services/flink-processor:/opt/flink/usrlib
    networks:
      - data-platform

volumes:
  flink-checkpoints:
  flink-savepoints:
```

### 1.5 Python Dependencies

File: `services/flink-processor/requirements.txt`

```
apache-flink==1.18.1
apache-flink-libraries==1.18.1
kafka-python==2.0.2
avro-python3==1.10.2
duckdb==1.1.0
pyarrow==15.0.0
fastavro==1.9.4
```

---

## 2. Flink Stream Processing Jobs

### 2.1 Job 1: Trade Event Router

**Purpose**: Classify incoming raw trade events and route them to dedicated downstream topics based on order status. This is the primary fan-out job that enables independent downstream consumers per trade lifecycle stage.

```
                                    +---> processed.trades.new       (NEW orders)
                                    |
  raw.trades  --->  [Router]  ------+---> processed.trades.filled    (FILLED orders)
                                    |
                                    +---> processed.trades.cancelled (CANCELLED orders)
                                    |
                                    +---> processed.trades.rejected  (REJECTED orders)
                                    |
                                    +---> dlq.trades.unroutable      (Unknown status)
```

**Input Topic**: `raw.trades`
**Input Schema** (Avro):
```json
{
  "type": "record",
  "name": "RawTradeEvent",
  "namespace": "com.stockplatform.trades",
  "fields": [
    {"name": "trade_id",       "type": "string"},
    {"name": "order_id",       "type": "string"},
    {"name": "symbol",         "type": "string"},
    {"name": "side",           "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
    {"name": "quantity",       "type": "double"},
    {"name": "price",          "type": "double"},
    {"name": "status",         "type": "string"},
    {"name": "exchange",       "type": "string"},
    {"name": "trader_id",      "type": "string"},
    {"name": "account_id",     "type": "string"},
    {"name": "timestamp_ms",   "type": "long"},
    {"name": "event_type",     "type": "string"}
  ]
}
```

**Processing Logic**:
```python
# services/flink-processor/jobs/trade_event_router.py

from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Row, Types
import json
import logging

logger = logging.getLogger(__name__)

# Define side outputs for each trade status
TAG_FILLED    = OutputTag("filled",    Types.STRING())
TAG_CANCELLED = OutputTag("cancelled", Types.STRING())
TAG_REJECTED  = OutputTag("rejected",  Types.STRING())
TAG_DLQ       = OutputTag("dlq",       Types.STRING())


class TradeRouterFunction(ProcessFunction):
    """Routes trade events to appropriate output streams based on order status."""

    VALID_STATUSES = {"NEW", "FILLED", "PARTIALLY_FILLED", "CANCELLED", "REJECTED"}

    def process_element(self, value, ctx: ProcessFunction.Context):
        try:
            trade = json.loads(value)
            status = trade.get("status", "").upper()

            if status == "NEW":
                # Main output: new orders
                yield trade
            elif status in ("FILLED", "PARTIALLY_FILLED"):
                yield TAG_FILLED, trade
            elif status == "CANCELLED":
                yield TAG_CANCELLED, trade
            elif status == "REJECTED":
                yield TAG_REJECTED, trade
            else:
                logger.warning(f"Unknown trade status: {status}, trade_id={trade.get('trade_id')}")
                yield TAG_DLQ, trade

        except json.JSONDecodeError as e:
            logger.error(f"Deserialization error: {e}")
            yield TAG_DLQ, value


def build_trade_router_job(env: StreamExecutionEnvironment, kafka_config: dict):
    """Constructs and returns the trade event router job graph."""

    # Source: raw.trades
    raw_trades = env.add_source(
        build_kafka_source(
            topic="raw.trades",
            group_id="flink-trade-router",
            config=kafka_config
        )
    ).name("raw-trades-source")

    # Apply routing logic
    routed = raw_trades.process(TradeRouterFunction()).name("trade-router")

    # Main output -> processed.trades.new
    routed.add_sink(
        build_kafka_sink(topic="processed.trades.new", config=kafka_config)
    ).name("sink-new-trades")

    # Side outputs -> respective topics
    routed.get_side_output(TAG_FILLED).add_sink(
        build_kafka_sink(topic="processed.trades.filled", config=kafka_config)
    ).name("sink-filled-trades")

    routed.get_side_output(TAG_CANCELLED).add_sink(
        build_kafka_sink(topic="processed.trades.cancelled", config=kafka_config)
    ).name("sink-cancelled-trades")

    routed.get_side_output(TAG_REJECTED).add_sink(
        build_kafka_sink(topic="processed.trades.rejected", config=kafka_config)
    ).name("sink-rejected-trades")

    routed.get_side_output(TAG_DLQ).add_sink(
        build_kafka_sink(topic="dlq.trades.unroutable", config=kafka_config)
    ).name("sink-dlq-trades")
```

**Output Topics**:

| Topic                        | Key           | Description              |
|------------------------------|---------------|--------------------------|
| `processed.trades.new`       | `order_id`    | New order placements     |
| `processed.trades.filled`    | `order_id`    | Filled / partial fills   |
| `processed.trades.cancelled` | `order_id`    | Cancellation events      |
| `processed.trades.rejected`  | `order_id`    | Rejected orders          |
| `dlq.trades.unroutable`      | `trade_id`    | Unclassifiable events    |

---

### 2.2 Job 2: Real-Time Trade Aggregation

**Purpose**: Compute rolling aggregations over tumbling windows at 1-minute, 5-minute, and 15-minute intervals. Produces per-symbol metrics including volume, VWAP, trade count, and buy/sell ratio.

```
  raw.trades  --->  [KeyBy symbol]  ---> [Window 1m ] ---> analytics.trade-aggregates
                                    ---> [Window 5m ] ---> analytics.trade-aggregates
                                    ---> [Window 15m] ---> analytics.trade-aggregates
```

**Input Topic**: `raw.trades`
**Output Topic**: `analytics.trade-aggregates`

**Aggregation Schema** (output):
```json
{
  "type": "record",
  "name": "TradeAggregate",
  "fields": [
    {"name": "symbol",          "type": "string"},
    {"name": "window_start",    "type": "long"},
    {"name": "window_end",      "type": "long"},
    {"name": "window_size",     "type": "string"},
    {"name": "total_volume",    "type": "double"},
    {"name": "total_value",     "type": "double"},
    {"name": "vwap",            "type": "double"},
    {"name": "trade_count",     "type": "long"},
    {"name": "buy_count",       "type": "long"},
    {"name": "sell_count",      "type": "long"},
    {"name": "buy_sell_ratio",  "type": "double"},
    {"name": "high_price",      "type": "double"},
    {"name": "low_price",       "type": "double"},
    {"name": "open_price",      "type": "double"},
    {"name": "close_price",     "type": "double"},
    {"name": "computed_at",     "type": "long"}
  ]
}
```

**Processing Logic**:
```python
# services/flink-processor/jobs/trade_aggregation.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import AggregateFunction
from pyflink.common import WatermarkStrategy, Duration
import json
import time


class TradeAggregator(AggregateFunction):
    """Accumulates trade metrics within a window for a single symbol."""

    def create_accumulator(self):
        return {
            "total_volume": 0.0,
            "total_value": 0.0,        # sum(price * quantity) for VWAP
            "trade_count": 0,
            "buy_count": 0,
            "sell_count": 0,
            "high_price": float("-inf"),
            "low_price": float("inf"),
            "first_price": None,
            "last_price": None,
            "first_timestamp": None,
            "last_timestamp": None,
        }

    def add(self, trade, accumulator):
        price = trade["price"]
        quantity = trade["quantity"]
        ts = trade["timestamp_ms"]

        accumulator["total_volume"] += quantity
        accumulator["total_value"] += price * quantity
        accumulator["trade_count"] += 1

        if trade["side"] == "BUY":
            accumulator["buy_count"] += 1
        else:
            accumulator["sell_count"] += 1

        accumulator["high_price"] = max(accumulator["high_price"], price)
        accumulator["low_price"] = min(accumulator["low_price"], price)

        if accumulator["first_timestamp"] is None or ts < accumulator["first_timestamp"]:
            accumulator["first_price"] = price
            accumulator["first_timestamp"] = ts

        if accumulator["last_timestamp"] is None or ts > accumulator["last_timestamp"]:
            accumulator["last_price"] = price
            accumulator["last_timestamp"] = ts

        return accumulator

    def get_result(self, accumulator):
        vwap = (
            accumulator["total_value"] / accumulator["total_volume"]
            if accumulator["total_volume"] > 0
            else 0.0
        )
        buy_sell_ratio = (
            accumulator["buy_count"] / accumulator["sell_count"]
            if accumulator["sell_count"] > 0
            else float("inf")
        )
        return {
            "total_volume": accumulator["total_volume"],
            "total_value": accumulator["total_value"],
            "vwap": round(vwap, 4),
            "trade_count": accumulator["trade_count"],
            "buy_count": accumulator["buy_count"],
            "sell_count": accumulator["sell_count"],
            "buy_sell_ratio": round(buy_sell_ratio, 4),
            "high_price": accumulator["high_price"],
            "low_price": accumulator["low_price"],
            "open_price": accumulator["first_price"],
            "close_price": accumulator["last_price"],
            "computed_at": int(time.time() * 1000),
        }

    def merge(self, acc_a, acc_b):
        acc_a["total_volume"] += acc_b["total_volume"]
        acc_a["total_value"] += acc_b["total_value"]
        acc_a["trade_count"] += acc_b["trade_count"]
        acc_a["buy_count"] += acc_b["buy_count"]
        acc_a["sell_count"] += acc_b["sell_count"]
        acc_a["high_price"] = max(acc_a["high_price"], acc_b["high_price"])
        acc_a["low_price"] = min(acc_a["low_price"], acc_b["low_price"])

        if acc_b["first_timestamp"] is not None:
            if acc_a["first_timestamp"] is None or acc_b["first_timestamp"] < acc_a["first_timestamp"]:
                acc_a["first_price"] = acc_b["first_price"]
                acc_a["first_timestamp"] = acc_b["first_timestamp"]

        if acc_b["last_timestamp"] is not None:
            if acc_a["last_timestamp"] is None or acc_b["last_timestamp"] > acc_a["last_timestamp"]:
                acc_a["last_price"] = acc_b["last_price"]
                acc_a["last_timestamp"] = acc_b["last_timestamp"]

        return acc_a


WINDOW_SIZES = [
    ("1m",  Time.minutes(1)),
    ("5m",  Time.minutes(5)),
    ("15m", Time.minutes(15)),
]


def build_trade_aggregation_job(env: StreamExecutionEnvironment, kafka_config: dict):
    """Builds parallel windowed aggregation pipelines for each window size."""

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda trade, _: trade["timestamp_ms"]
        )
    )

    raw_trades = (
        env.add_source(
            build_kafka_source(
                topic="raw.trades",
                group_id="flink-trade-aggregator",
                config=kafka_config,
            )
        )
        .map(lambda x: json.loads(x))
        .assign_timestamps_and_watermarks(watermark_strategy)
        .name("raw-trades-with-watermarks")
    )

    for window_label, window_size in WINDOW_SIZES:
        (
            raw_trades
            .key_by(lambda trade: trade["symbol"])
            .window(TumblingEventTimeWindows.of(window_size))
            .allowed_lateness(Time.seconds(30))
            .aggregate(
                TradeAggregator(),
                window_function=attach_window_metadata(window_label),
            )
            .name(f"aggregate-{window_label}")
            .add_sink(
                build_kafka_sink(
                    topic="analytics.trade-aggregates",
                    config=kafka_config,
                )
            )
            .name(f"sink-aggregates-{window_label}")
        )
```

---

### 2.3 Job 3: Price Alert Detection

**Purpose**: Detect significant price movements (greater than 2% change within a 5-minute sliding window) using pattern matching. Generates real-time alerts for downstream consumers such as notification services and dashboards.

```
  raw.trades       ---+
                      |---> [Join on symbol] ---> [Pattern Detect] ---> alerts.price-movement
  raw.marketdata   ---+
```

**Input Topics**: `raw.trades`, `raw.marketdata`
**Output Topic**: `alerts.price-movement`

**Detection Logic**:
- Maintain a rolling price baseline per symbol using the first trade price in each 5-minute window.
- Compare each subsequent trade price against the baseline.
- Fire an alert when the absolute percentage change exceeds the configured threshold (default 2%).
- Include a cooldown period of 60 seconds per symbol to avoid alert storms.

**Processing Logic**:
```python
# services/flink-processor/jobs/price_alert_detection.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import Types
import json
import time
import logging

logger = logging.getLogger(__name__)

PRICE_CHANGE_THRESHOLD = 0.02   # 2%
DETECTION_WINDOW_MS    = 300000 # 5 minutes
COOLDOWN_MS            = 60000  # 1 minute cooldown between alerts per symbol


class PriceAlertDetector(KeyedProcessFunction):
    """
    Detects significant price movements per symbol.

    State:
      - baseline_price: The reference price at the start of the detection window.
      - baseline_timestamp: When the baseline was established.
      - last_alert_timestamp: When the last alert was fired (for cooldown).
    """

    def open(self, runtime_context):
        self.baseline_price = runtime_context.get_state(
            ValueStateDescriptor("baseline_price", Types.DOUBLE())
        )
        self.baseline_timestamp = runtime_context.get_state(
            ValueStateDescriptor("baseline_timestamp", Types.LONG())
        )
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor("last_alert_time", Types.LONG())
        )

    def process_element(self, trade, ctx: KeyedProcessFunction.Context):
        current_price = trade["price"]
        current_ts = trade["timestamp_ms"]
        symbol = trade["symbol"]

        baseline = self.baseline_price.value()
        baseline_ts = self.baseline_timestamp.value()

        # Initialize baseline if not set or window expired
        if baseline is None or (current_ts - baseline_ts) > DETECTION_WINDOW_MS:
            self.baseline_price.update(current_price)
            self.baseline_timestamp.update(current_ts)
            return

        # Calculate percentage change
        pct_change = abs(current_price - baseline) / baseline

        if pct_change >= PRICE_CHANGE_THRESHOLD:
            # Check cooldown
            last_alert = self.last_alert_time.value()
            if last_alert is not None and (current_ts - last_alert) < COOLDOWN_MS:
                return

            direction = "UP" if current_price > baseline else "DOWN"
            alert = {
                "alert_type": "PRICE_MOVEMENT",
                "symbol": symbol,
                "direction": direction,
                "baseline_price": baseline,
                "current_price": current_price,
                "pct_change": round(pct_change * 100, 2),
                "window_start_ms": baseline_ts,
                "detected_at_ms": current_ts,
                "severity": classify_severity(pct_change),
            }

            self.last_alert_time.update(current_ts)
            # Reset baseline after alert
            self.baseline_price.update(current_price)
            self.baseline_timestamp.update(current_ts)

            logger.info(f"Price alert: {symbol} {direction} {alert['pct_change']}%")
            yield json.dumps(alert)


def classify_severity(pct_change: float) -> str:
    """Classify alert severity based on magnitude of price change."""
    if pct_change >= 0.10:
        return "CRITICAL"
    elif pct_change >= 0.05:
        return "HIGH"
    elif pct_change >= 0.02:
        return "MEDIUM"
    return "LOW"


def build_price_alert_job(env: StreamExecutionEnvironment, kafka_config: dict):
    """Builds the price alert detection pipeline."""

    trades_stream = (
        env.add_source(
            build_kafka_source(topic="raw.trades", group_id="flink-price-alerts", config=kafka_config)
        )
        .map(lambda x: json.loads(x))
        .assign_timestamps_and_watermarks(bounded_watermark_strategy(5))
        .name("raw-trades-for-alerts")
    )

    market_data_stream = (
        env.add_source(
            build_kafka_source(topic="raw.marketdata", group_id="flink-price-alerts", config=kafka_config)
        )
        .map(lambda x: json.loads(x))
        .assign_timestamps_and_watermarks(bounded_watermark_strategy(5))
        .name("raw-marketdata-for-alerts")
    )

    # Union both streams (both carry symbol + price + timestamp_ms)
    unified = trades_stream.union(market_data_stream)

    (
        unified
        .key_by(lambda event: event["symbol"])
        .process(PriceAlertDetector())
        .name("price-alert-detector")
        .add_sink(
            build_kafka_sink(topic="alerts.price-movement", config=kafka_config)
        )
        .name("sink-price-alerts")
    )
```

**Alert Output Schema**:
```json
{
  "alert_type": "PRICE_MOVEMENT",
  "symbol": "AAPL",
  "direction": "DOWN",
  "baseline_price": 185.50,
  "current_price": 181.20,
  "pct_change": 2.32,
  "window_start_ms": 1700000000000,
  "detected_at_ms": 1700000300000,
  "severity": "MEDIUM"
}
```

---

### 2.4 Job 4: Order Book Analytics

**Purpose**: Consume order book snapshots, compute bid-ask spread, order book depth at configurable levels, and order book imbalance. These metrics feed the analytics dashboard and are used for market microstructure analysis.

```
  raw.orderbook  --->  [KeyBy symbol]  --->  [Compute Metrics]  --->  analytics.orderbook
```

**Input Topic**: `raw.orderbook`
**Output Topic**: `analytics.orderbook`

**Input Schema**:
```json
{
  "symbol": "AAPL",
  "timestamp_ms": 1700000000000,
  "bids": [
    {"price": 185.40, "quantity": 500},
    {"price": 185.35, "quantity": 300}
  ],
  "asks": [
    {"price": 185.45, "quantity": 200},
    {"price": 185.50, "quantity": 400}
  ]
}
```

**Processing Logic**:
```python
# services/flink-processor/jobs/orderbook_analytics.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
import json


class OrderBookAnalyzer(MapFunction):
    """Computes real-time order book metrics per snapshot."""

    DEPTH_LEVELS = [5, 10, 20]

    def map(self, value):
        book = json.loads(value)
        bids = book.get("bids", [])
        asks = book.get("asks", [])

        # Best bid / ask
        best_bid = bids[0]["price"] if bids else 0.0
        best_ask = asks[0]["price"] if asks else 0.0

        # Bid-ask spread
        spread = best_ask - best_bid if (best_bid > 0 and best_ask > 0) else 0.0
        mid_price = (best_bid + best_ask) / 2 if (best_bid > 0 and best_ask > 0) else 0.0
        spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0.0

        # Depth at levels
        depth_metrics = {}
        for level in self.DEPTH_LEVELS:
            bid_depth = sum(b["quantity"] for b in bids[:level])
            ask_depth = sum(a["quantity"] for a in asks[:level])
            total_depth = bid_depth + ask_depth
            imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0

            depth_metrics[f"bid_depth_{level}"] = bid_depth
            depth_metrics[f"ask_depth_{level}"] = ask_depth
            depth_metrics[f"imbalance_{level}"] = round(imbalance, 4)

        result = {
            "symbol": book["symbol"],
            "timestamp_ms": book["timestamp_ms"],
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": round(mid_price, 4),
            "spread": round(spread, 4),
            "spread_bps": round(spread_bps, 2),
            **depth_metrics,
        }

        return json.dumps(result)
```

**Output Schema**:
```json
{
  "symbol": "AAPL",
  "timestamp_ms": 1700000000000,
  "best_bid": 185.40,
  "best_ask": 185.45,
  "mid_price": 185.425,
  "spread": 0.05,
  "spread_bps": 2.70,
  "bid_depth_5": 2500,
  "ask_depth_5": 1800,
  "imbalance_5": 0.1628,
  "bid_depth_10": 5000,
  "ask_depth_10": 4200,
  "imbalance_10": 0.0870,
  "bid_depth_20": 10000,
  "ask_depth_20": 9500,
  "imbalance_20": 0.0256
}
```

---

### 2.5 Job 5: Trade Enrichment

**Purpose**: Enrich filled trade events with reference data including company name, sector, industry, and market cap tier. Reference data is loaded as a side input from a local CSV/Parquet file and refreshed periodically via a broadcast stream pattern.

```
  processed.trades.filled  ---+
                               |---> [Connect + Enrich] ---> enriched.trades
  Reference Data (broadcast) --+
```

**Input Topic**: `processed.trades.filled`
**Side Input**: Reference data file (`data/reference/symbols.parquet`)
**Output Topic**: `enriched.trades`

**Processing Logic**:
```python
# services/flink-processor/jobs/trade_enrichment.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    BroadcastProcessFunction,
    KeyedBroadcastProcessFunction,
)
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common import Types
import json
import duckdb
import logging

logger = logging.getLogger(__name__)

# Broadcast state descriptor for reference data
REFERENCE_STATE_DESC = MapStateDescriptor(
    "reference_data",
    Types.STRING(),   # symbol
    Types.STRING(),   # JSON blob of reference attributes
)


def load_reference_data(parquet_path: str) -> dict:
    """Load reference data from Parquet file using DuckDB."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT symbol, company_name, sector, industry, market_cap_tier, exchange
        FROM read_parquet('{parquet_path}')
    """).fetchdf()
    con.close()

    reference = {}
    for _, row in df.iterrows():
        reference[row["symbol"]] = {
            "company_name": row["company_name"],
            "sector": row["sector"],
            "industry": row["industry"],
            "market_cap_tier": row["market_cap_tier"],
            "exchange": row["exchange"],
        }
    return reference


class TradeEnrichmentFunction(KeyedBroadcastProcessFunction):
    """Enriches trades with reference data from broadcast state."""

    def process_element(self, trade, ctx):
        symbol = trade["symbol"]
        ref_state = ctx.get_broadcast_state(REFERENCE_STATE_DESC)
        ref_json = ref_state.get(symbol)

        enriched = dict(trade)

        if ref_json:
            ref_data = json.loads(ref_json)
            enriched["company_name"]    = ref_data.get("company_name", "UNKNOWN")
            enriched["sector"]          = ref_data.get("sector", "UNKNOWN")
            enriched["industry"]        = ref_data.get("industry", "UNKNOWN")
            enriched["market_cap_tier"] = ref_data.get("market_cap_tier", "UNKNOWN")
            enriched["listed_exchange"] = ref_data.get("exchange", "UNKNOWN")
        else:
            enriched["company_name"]    = "UNKNOWN"
            enriched["sector"]          = "UNKNOWN"
            enriched["industry"]        = "UNKNOWN"
            enriched["market_cap_tier"] = "UNKNOWN"
            enriched["listed_exchange"] = "UNKNOWN"
            logger.warning(f"No reference data for symbol: {symbol}")

        enriched["enriched_at_ms"] = int(time.time() * 1000)
        yield json.dumps(enriched)

    def process_broadcast_element(self, ref_update, ctx):
        """Handle reference data updates broadcast to all operators."""
        ref_state = ctx.get_broadcast_state(REFERENCE_STATE_DESC)
        data = json.loads(ref_update)
        ref_state.put(data["symbol"], json.dumps(data))
```

---

## 3. Kafka Topic Architecture

### 3.1 Complete Topic Map

#### Raw Topics (Ingestion Layer)

| Topic             | Partitions | Retention | Key        | Value Schema         | Description                        |
|-------------------|------------|-----------|------------|----------------------|------------------------------------|
| `raw.trades`      | 8          | 7 days    | `symbol`   | Avro: RawTradeEvent  | All trade events from WebSocket    |
| `raw.marketdata`  | 8          | 3 days    | `symbol`   | Avro: MarketDataTick | Real-time price quotes             |
| `raw.orderbook`   | 8          | 1 day     | `symbol`   | Avro: OrderBookSnap  | Order book snapshots               |

#### Processed Topics (Routing Layer)

| Topic                        | Partitions | Retention | Key        | Value Schema           | Description                    |
|------------------------------|------------|-----------|------------|------------------------|--------------------------------|
| `processed.trades.new`       | 4          | 7 days    | `order_id` | Avro: RawTradeEvent    | New order placements           |
| `processed.trades.filled`    | 4          | 14 days   | `order_id` | Avro: RawTradeEvent    | Filled orders (incl. partial)  |
| `processed.trades.cancelled` | 4          | 7 days    | `order_id` | Avro: RawTradeEvent    | Cancelled orders               |
| `processed.trades.rejected`  | 2          | 7 days    | `order_id` | Avro: RawTradeEvent    | Rejected orders                |

#### Analytics Topics (Compute Layer)

| Topic                         | Partitions | Retention | Key        | Value Schema           | Description                        |
|-------------------------------|------------|-----------|------------|------------------------|------------------------------------|
| `analytics.trade-aggregates`  | 4          | 30 days   | `symbol`   | Avro: TradeAggregate   | Windowed trade aggregations        |
| `analytics.orderbook`         | 4          | 3 days    | `symbol`   | Avro: OrderBookMetrics | Order book analytics               |

#### Enriched Topics (Enrichment Layer)

| Topic              | Partitions | Retention | Key        | Value Schema           | Description                        |
|--------------------|------------|-----------|------------|------------------------|------------------------------------|
| `enriched.trades`  | 4          | 30 days   | `order_id` | Avro: EnrichedTrade    | Trades with reference data         |

#### Alert Topics (Detection Layer)

| Topic                    | Partitions | Retention | Key        | Value Schema       | Description                        |
|--------------------------|------------|-----------|------------|--------------------|------------------------------------|
| `alerts.price-movement`  | 2          | 7 days    | `symbol`   | Avro: PriceAlert   | Significant price change alerts    |

#### Dead Letter Topics (Error Handling)

| Topic                    | Partitions | Retention | Key        | Value Schema   | Description                          |
|--------------------------|------------|-----------|------------|----------------|--------------------------------------|
| `dlq.trades.unroutable`  | 2          | 30 days   | `trade_id` | String (raw)   | Events that failed routing           |
| `dlq.deserialization`    | 2          | 30 days   | none       | String (raw)   | Events that failed deserialization   |
| `dlq.processing-errors`  | 2          | 30 days   | none       | String (raw)   | Events that caused processing errors |

### 3.2 Topic Creation Script

File: `infrastructure/kafka/create-topics.sh`

```bash
#!/bin/bash
KAFKA_BOOTSTRAP="localhost:9092"

declare -A TOPICS=(
  # Raw topics
  ["raw.trades"]="8:7"
  ["raw.marketdata"]="8:3"
  ["raw.orderbook"]="8:1"

  # Processed topics
  ["processed.trades.new"]="4:7"
  ["processed.trades.filled"]="4:14"
  ["processed.trades.cancelled"]="4:7"
  ["processed.trades.rejected"]="2:7"

  # Analytics topics
  ["analytics.trade-aggregates"]="4:30"
  ["analytics.orderbook"]="4:3"

  # Enriched topics
  ["enriched.trades"]="4:30"

  # Alert topics
  ["alerts.price-movement"]="2:7"

  # Dead letter topics
  ["dlq.trades.unroutable"]="2:30"
  ["dlq.deserialization"]="2:30"
  ["dlq.processing-errors"]="2:30"
)

for topic in "${!TOPICS[@]}"; do
  IFS=':' read -r partitions retention_days <<< "${TOPICS[$topic]}"
  retention_ms=$((retention_days * 86400000))

  kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$retention_ms" \
    --config cleanup.policy=delete

  echo "Created topic: $topic (partitions=$partitions, retention=${retention_days}d)"
done
```

### 3.3 Naming Convention

```
<layer>.<domain>.<entity>[.<qualifier>]

Layers:
  raw.*          - Unprocessed ingestion data
  processed.*    - Routed / filtered data
  analytics.*    - Computed metrics and aggregations
  enriched.*     - Data joined with reference data
  alerts.*       - Detection and notification events
  dlq.*          - Dead letter queues for error handling
```

---

## 4. State Management

### 4.1 Keyed State (Per-Symbol)

Keyed state is partitioned by the key of the stream (typically `symbol`). Each key gets its own isolated state instance, enabling parallel processing of independent symbols.

| Job                    | State Type    | State Key  | Contents                                    | TTL       |
|------------------------|---------------|------------|---------------------------------------------|-----------|
| Trade Aggregation      | ValueState    | `symbol`   | Running accumulator per window               | Window    |
| Price Alert Detection  | ValueState    | `symbol`   | Baseline price, last alert timestamp         | 10 min    |
| Order Book Analytics   | ValueState    | `symbol`   | Previous snapshot for delta computation      | 5 min     |
| Trade Enrichment       | BroadcastState| `symbol`   | Reference data map                           | None      |

### 4.2 State TTL Configuration

```python
# services/flink-processor/config/state_config.py

from pyflink.datastream.state import StateTtlConfig
from pyflink.common import Time


def create_ttl_config(ttl_minutes: int) -> StateTtlConfig:
    """Create a state TTL configuration.

    - State entries expire after the specified TTL.
    - TTL is refreshed on both read and write access.
    - Expired entries are cleaned up lazily during state access
      and proactively during full snapshots.
    """
    return (
        StateTtlConfig.new_builder(Time.minutes(ttl_minutes))
        .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)
        .set_state_visibility(
            StateTtlConfig.StateVisibility.NeverReturnExpired
        )
        .cleanup_incrementally(100, True)   # Check 100 entries per access
        .cleanup_in_rocksdb_compact_filter(1000)
        .build()
    )


# Pre-configured TTL settings per job
PRICE_ALERT_STATE_TTL   = create_ttl_config(ttl_minutes=10)
ORDERBOOK_STATE_TTL     = create_ttl_config(ttl_minutes=5)
ENRICHMENT_CACHE_TTL    = create_ttl_config(ttl_minutes=60)
```

### 4.3 Savepoints for Job Upgrades

Savepoints are consistent snapshots of the entire job state. They are taken before any job modification (code update, configuration change, parallelism change) to allow seamless restarts without data loss.

**Savepoint workflow**:

```bash
# 1. Trigger a savepoint before stopping the job
flink savepoint <job-id> file:///opt/flink/savepoints/

# 2. Cancel the job with savepoint (atomic)
flink cancel -s file:///opt/flink/savepoints/ <job-id>

# 3. Restart from savepoint after upgrade
flink run -s file:///opt/flink/savepoints/savepoint-<id> \
  /opt/flink/usrlib/jobs/trade_event_router.py

# 4. Verify the restored job
flink list -r
```

**Savepoint compatibility rules**:
- All stateful operators must have stable UIDs assigned via `.uid("operator-name")`.
- Adding new operators is safe.
- Removing a stateful operator requires `--allowNonRestoredState`.
- Changing key serializers is not supported; requires state migration.

**UID Assignment Convention**:
```python
# Every stateful operator MUST have a UID for savepoint compatibility
(
    stream
    .key_by(lambda t: t["symbol"])
    .process(PriceAlertDetector())
    .uid("price-alert-detector-v1")     # <-- Stable UID
    .name("price-alert-detector")       # <-- Display name in Web UI
)
```

### 4.4 RocksDB State Backend Tuning

```yaml
# Additional RocksDB tuning for large state
state.backend.rocksdb.block.cache-size: 256mb       # Read cache
state.backend.rocksdb.writebuffer.size: 128mb        # Write buffer
state.backend.rocksdb.writebuffer.count: 4           # Number of write buffers
state.backend.rocksdb.compaction.level.max-size-level-base: 320mb
state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
```

---

## 5. DuckDB Integration

### 5.1 Role in the Architecture

DuckDB serves as the ad-hoc analytical query engine that complements Flink's continuous stream processing. While Flink handles real-time, always-on processing, DuckDB is used for interactive analysis, reference data loading, and batch analytics over materialized data in Parquet/Iceberg format.

```
  Flink (real-time)                    DuckDB (ad-hoc)
  +------------------+                +------------------+
  | Continuous        |   Parquet/    | Interactive       |
  | Stream Processing |--  Iceberg -->| Analytical        |
  | (always running)  |   files      | Queries (on-demand)|
  +------------------+                +------------------+
```

### 5.2 Use Cases

| Use Case                   | Description                                                       | Data Source                  |
|----------------------------|-------------------------------------------------------------------|------------------------------|
| Reference data lookups     | Load symbol reference data for Flink enrichment job               | `data/reference/*.parquet`   |
| Ad-hoc recent data queries | Explore the last N minutes/hours of trade data interactively      | `data/warehouse/*.parquet`   |
| Batch analytics            | Run complex analytics that complement streaming aggregations      | Iceberg tables               |
| Historical comparison      | Compare current metrics against historical baselines              | `data/warehouse/*.parquet`   |
| Data quality validation    | Spot-check data consistency after Flink processing                | Multiple Parquet files       |

### 5.3 Python Integration

File: `services/analytics/duckdb_analytics.py`

```python
"""
DuckDB-based ad-hoc analytics engine for the stock trading platform.

Provides interactive query capabilities over materialized Parquet/Iceberg
data produced by the Flink streaming pipeline.
"""

import duckdb
from pathlib import Path
from typing import Optional
import pandas as pd

# Base directory for warehouse data
WAREHOUSE_DIR = Path("data/warehouse")
REFERENCE_DIR = Path("data/reference")


class TradingAnalytics:
    """Ad-hoc analytics engine powered by DuckDB."""

    def __init__(self, database: str = ":memory:", read_only: bool = False):
        self.con = duckdb.connect(database=database, read_only=read_only)
        self._configure()

    def _configure(self):
        """Apply optimal DuckDB settings for analytics workloads."""
        self.con.execute("SET memory_limit = '2GB'")
        self.con.execute("SET threads = 4")
        self.con.execute("SET enable_progress_bar = true")
        # Install and load Iceberg extension for reading Iceberg tables
        self.con.execute("INSTALL iceberg")
        self.con.execute("LOAD iceberg")

    def close(self):
        self.con.close()

    # ---- Reference Data ----

    def load_reference_data(self, parquet_path: Optional[str] = None) -> pd.DataFrame:
        """Load symbol reference data for enrichment lookups."""
        path = parquet_path or str(REFERENCE_DIR / "symbols.parquet")
        return self.con.execute(f"""
            SELECT symbol, company_name, sector, industry,
                   market_cap_tier, exchange
            FROM read_parquet('{path}')
            ORDER BY symbol
        """).fetchdf()

    # ---- Top Traded Symbols ----

    def top_traded_symbols(
        self,
        parquet_glob: str = "data/warehouse/trades/**/*.parquet",
        top_n: int = 20,
        since_hours: int = 24,
    ) -> pd.DataFrame:
        """
        Find the most actively traded symbols by volume and trade count.

        Args:
            parquet_glob: Glob pattern for trade Parquet files.
            top_n: Number of top symbols to return.
            since_hours: Lookback window in hours.
        """
        return self.con.execute(f"""
            WITH recent_trades AS (
                SELECT *
                FROM read_parquet('{parquet_glob}', hive_partitioning=true)
                WHERE timestamp_ms >= (
                    epoch_ms(now()) - ({since_hours} * 3600 * 1000)
                )
            )
            SELECT
                symbol,
                COUNT(*)                          AS trade_count,
                SUM(quantity)                     AS total_volume,
                SUM(price * quantity)             AS total_notional,
                ROUND(SUM(price * quantity)
                      / NULLIF(SUM(quantity), 0), 4)  AS vwap,
                MIN(price)                        AS low_price,
                MAX(price)                        AS high_price,
                ROUND(
                    (MAX(price) - MIN(price))
                    / NULLIF(MIN(price), 0) * 100, 2
                )                                 AS price_range_pct
            FROM recent_trades
            WHERE status = 'FILLED'
            GROUP BY symbol
            ORDER BY total_volume DESC
            LIMIT {top_n}
        """).fetchdf()

    # ---- Portfolio Analysis ----

    def portfolio_analysis(
        self,
        account_id: str,
        parquet_glob: str = "data/warehouse/trades/**/*.parquet",
    ) -> pd.DataFrame:
        """
        Analyze current position and P&L for a given account.

        Computes net position per symbol by summing buys and subtracting sells.
        """
        return self.con.execute(f"""
            WITH filled_trades AS (
                SELECT *
                FROM read_parquet('{parquet_glob}', hive_partitioning=true)
                WHERE account_id = '{account_id}'
                  AND status = 'FILLED'
            ),
            positions AS (
                SELECT
                    symbol,
                    SUM(CASE WHEN side = 'BUY'  THEN quantity ELSE 0 END) AS bought_qty,
                    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) AS sold_qty,
                    SUM(CASE
                        WHEN side = 'BUY'  THEN  price * quantity
                        WHEN side = 'SELL' THEN -price * quantity
                    END) AS net_cost,
                    COUNT(*) AS trade_count
                FROM filled_trades
                GROUP BY symbol
            )
            SELECT
                symbol,
                bought_qty,
                sold_qty,
                (bought_qty - sold_qty) AS net_position,
                ROUND(net_cost, 2) AS net_cost,
                ROUND(
                    net_cost / NULLIF(bought_qty - sold_qty, 0), 4
                ) AS avg_cost_basis,
                trade_count
            FROM positions
            WHERE (bought_qty - sold_qty) != 0
            ORDER BY ABS(net_cost) DESC
        """).fetchdf()

    # ---- Historical Comparison ----

    def compare_volume_to_historical(
        self,
        symbol: str,
        parquet_glob: str = "data/warehouse/aggregates/**/*.parquet",
    ) -> pd.DataFrame:
        """
        Compare today's trading volume for a symbol against its
        historical 7-day, 30-day, and 90-day averages.
        """
        return self.con.execute(f"""
            WITH daily_volumes AS (
                SELECT
                    symbol,
                    DATE_TRUNC('day', to_timestamp(window_start / 1000)) AS trade_date,
                    SUM(total_volume) AS daily_volume
                FROM read_parquet('{parquet_glob}', hive_partitioning=true)
                WHERE symbol = '{symbol}'
                  AND window_size = '1m'
                GROUP BY 1, 2
            ),
            today AS (
                SELECT daily_volume AS today_volume
                FROM daily_volumes
                WHERE trade_date = CURRENT_DATE
            ),
            averages AS (
                SELECT
                    AVG(CASE
                        WHEN trade_date >= CURRENT_DATE - INTERVAL 7 DAY
                        THEN daily_volume END) AS avg_7d,
                    AVG(CASE
                        WHEN trade_date >= CURRENT_DATE - INTERVAL 30 DAY
                        THEN daily_volume END) AS avg_30d,
                    AVG(CASE
                        WHEN trade_date >= CURRENT_DATE - INTERVAL 90 DAY
                        THEN daily_volume END) AS avg_90d
                FROM daily_volumes
                WHERE trade_date < CURRENT_DATE
            )
            SELECT
                '{symbol}' AS symbol,
                today.today_volume,
                ROUND(averages.avg_7d, 0)  AS avg_volume_7d,
                ROUND(averages.avg_30d, 0) AS avg_volume_30d,
                ROUND(averages.avg_90d, 0) AS avg_volume_90d,
                ROUND((today.today_volume / NULLIF(averages.avg_30d, 0) - 1) * 100, 2)
                    AS pct_vs_30d_avg
            FROM today, averages
        """).fetchdf()

    # ---- Sector Heatmap ----

    def sector_performance(
        self,
        parquet_glob: str = "data/warehouse/trades/**/*.parquet",
        reference_path: Optional[str] = None,
        since_hours: int = 24,
    ) -> pd.DataFrame:
        """Aggregate trade metrics by sector for heatmap visualization."""
        ref_path = reference_path or str(REFERENCE_DIR / "symbols.parquet")
        return self.con.execute(f"""
            WITH trades AS (
                SELECT *
                FROM read_parquet('{parquet_glob}', hive_partitioning=true)
                WHERE status = 'FILLED'
                  AND timestamp_ms >= (epoch_ms(now()) - ({since_hours} * 3600 * 1000))
            ),
            ref AS (
                SELECT symbol, sector, industry
                FROM read_parquet('{ref_path}')
            )
            SELECT
                ref.sector,
                COUNT(*)            AS trade_count,
                SUM(t.quantity)     AS total_volume,
                COUNT(DISTINCT t.symbol) AS symbol_count
            FROM trades t
            JOIN ref ON t.symbol = ref.symbol
            GROUP BY ref.sector
            ORDER BY total_volume DESC
        """).fetchdf()
```

### 5.4 Integration with Flink Pipeline

DuckDB is used at two points in the pipeline:

1. **Pre-flight**: The trade enrichment job uses DuckDB to load reference data from Parquet into memory before broadcasting it to Flink operators. This happens once at job startup and can be refreshed on a schedule.

2. **Post-flight**: After Flink writes aggregated/enriched data to Parquet/Iceberg tables, DuckDB queries those files for ad-hoc analysis, dashboards, and API responses.

```
                    DuckDB loads reference data
                           |
                           v
  raw.trades --> [Flink Pipeline] --> Parquet/Iceberg files
                                            |
                                            v
                                   DuckDB ad-hoc queries
                                            |
                                            v
                                   API layer / Dashboards
```

---

## 6. Exactly-Once Semantics

### 6.1 End-to-End Guarantee Chain

Exactly-once processing requires coordination across all three components: Kafka source, Flink processing, and Kafka sink. Each component implements its part of the guarantee.

```
  Kafka Source              Flink Processing             Kafka Sink
  +------------------+     +--------------------+     +------------------+
  | Committed offsets |<--->| Checkpointing      |<--->| Transactional    |
  | (consumer group)  |     | (exactly-once mode) |     | producer         |
  +------------------+     +--------------------+     +------------------+
         ^                         |                         |
         |                         v                         |
         +------------- Checkpoint barrier -----------------+
                    (atomic snapshot of offsets + state + txn)
```

### 6.2 Kafka Source Configuration

```python
# Consumer configuration for exactly-once source
kafka_source_config = {
    "bootstrap.servers": "kafka-broker-1:9092,kafka-broker-2:9092",
    "group.id": "flink-trade-processor",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "false",     # Flink manages offsets via checkpoints
    "isolation.level": "read_committed", # Only read committed messages
    "max.poll.records": 500,
}
```

Key behaviors:
- Flink does NOT use Kafka's auto-commit. Instead, consumer offsets are committed as part of each Flink checkpoint.
- On recovery, Flink restores the last checkpointed offsets and replays from that position.
- `read_committed` isolation ensures the source does not read uncommitted messages from upstream transactional producers.

### 6.3 Flink Checkpointing

```python
# services/flink-processor/config/env_config.py

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.common import Duration


def configure_environment() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()

    # Checkpointing
    env.enable_checkpointing(30000)  # Every 30 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(10000)  # 10s minimum
    checkpoint_config.set_checkpoint_timeout(120000)             # 2min timeout
    checkpoint_config.set_max_concurrent_checkpoints(1)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)

    # Retain checkpoints on cancellation for manual recovery
    checkpoint_config.enable_externalized_checkpoints(
        checkpoint_config.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )

    # Unaligned checkpoints reduce checkpoint duration under backpressure
    checkpoint_config.enable_unaligned_checkpoints()

    return env
```

Checkpoint process:
1. JobManager injects a checkpoint barrier into all source operators.
2. Barriers flow through the dataflow graph alongside regular records.
3. When an operator receives barriers from all input channels, it snapshots its state.
4. Once all operators have completed their snapshots, the checkpoint is considered complete.
5. The checkpoint includes: Kafka consumer offsets, operator state, and pending Kafka transactions.

### 6.4 Kafka Sink Configuration (Transactional Producer)

```python
# Sink configuration for exactly-once delivery
kafka_sink_config = {
    "bootstrap.servers": "kafka-broker-1:9092,kafka-broker-2:9092",
    "transaction.timeout.ms": "900000",     # 15 minutes (must be < broker max)
    "enable.idempotence": "true",           # Idempotent producer
    "acks": "all",                          # All replicas must acknowledge
    "retries": "3",
    "max.in.flight.requests.per.connection": "5",  # Safe with idempotence enabled
}
```

Key behaviors:
- The Flink Kafka sink uses Kafka transactions. Each checkpoint cycle opens a new transaction.
- Records produced between checkpoints are part of the open transaction.
- On successful checkpoint completion, the transaction is committed.
- On failure/recovery, the open transaction is aborted, and no duplicate records appear downstream.
- Downstream consumers using `read_committed` only see committed records.

### 6.5 Failure Scenarios and Recovery

| Scenario                        | Behavior                                                                        |
|---------------------------------|---------------------------------------------------------------------------------|
| TaskManager crash               | Job restarts from last checkpoint; Kafka offsets restored; sink txn aborted     |
| Checkpoint timeout              | Checkpoint discarded; next checkpoint attempted; no data loss                   |
| Kafka broker temporary failure  | Producer retries with idempotence; no duplicates                               |
| Network partition               | Checkpoint fails; job continues; recovers on next successful checkpoint        |
| JobManager failover (HA mode)   | Standby JM takes over; restores from last completed checkpoint                 |

---

## 7. Error Handling

### 7.1 Error Handling Architecture

```
  Input Event
       |
       v
  [Deserialize] ---error---> dlq.deserialization
       |
       v
  [Process]     ---error---> dlq.processing-errors  (via side output)
       |
       v
  [Late Data]   ---late----> side output / allowed lateness window
       |
       v
  Output Sink
```

### 7.2 Deserialization Error Handling

Events that fail JSON/Avro deserialization are routed to a dead letter topic without stopping the job. A custom deserialization schema wraps the actual deserializer and catches all exceptions.

```python
# services/flink-processor/common/safe_deserializer.py

from pyflink.datastream.functions import MapFunction
from pyflink.datastream import OutputTag
from pyflink.common import Types
import json
import logging
import traceback

logger = logging.getLogger(__name__)

TAG_DESER_ERROR = OutputTag("deserialization-error", Types.STRING())


class SafeJsonDeserializer(MapFunction):
    """
    Attempts JSON deserialization. On failure, routes the raw bytes
    to a dead letter side output with error metadata.
    """

    def open(self, runtime_context):
        self.error_counter = runtime_context.get_metrics_group().counter(
            "deserialization_errors"
        )

    def map(self, raw_value):
        try:
            parsed = json.loads(raw_value)
            return parsed
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.error_counter.inc()
            error_record = {
                "original_value": str(raw_value)[:10000],  # Truncate large payloads
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stacktrace": traceback.format_exc(),
                "timestamp_ms": int(time.time() * 1000),
            }
            logger.error(f"Deserialization error: {e}")
            # In practice, use a ProcessFunction with side outputs
            # This is simplified for illustration
            return None  # Filtered downstream
```

### 7.3 Processing Exception Handling

Business logic errors (null fields, invalid values, division by zero) are caught per-record and routed to side outputs. The job never fails due to a single bad record.

```python
# services/flink-processor/common/error_handler.py

from pyflink.datastream import OutputTag
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Types
import json
import time
import traceback
import logging

logger = logging.getLogger(__name__)

TAG_PROCESSING_ERROR = OutputTag("processing-error", Types.STRING())


class SafeProcessFunction(ProcessFunction):
    """
    Base class for all processing functions.
    Wraps process_element in a try/catch and routes errors
    to a dead letter side output.
    """

    def process_element(self, value, ctx):
        try:
            yield from self.safe_process(value, ctx)
        except Exception as e:
            error_record = {
                "original_event": value if isinstance(value, str) else json.dumps(value),
                "job_name": self.__class__.__name__,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "stacktrace": traceback.format_exc(),
                "timestamp_ms": int(time.time() * 1000),
            }
            logger.error(
                f"Processing error in {self.__class__.__name__}: {e}",
                exc_info=True,
            )
            yield TAG_PROCESSING_ERROR, json.dumps(error_record)

    def safe_process(self, value, ctx):
        """Override this method in subclasses with business logic."""
        raise NotImplementedError
```

### 7.4 Late Data Handling

Late-arriving events (events whose event-time timestamp falls before the current watermark) are handled through a combination of allowed lateness and side outputs.

```python
# Watermark strategy: bounded out-of-orderness with 5 seconds tolerance
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(lambda event, _: event["timestamp_ms"])
    .with_idleness(Duration.of_minutes(1))  # Handle idle partitions
)

# Window with allowed lateness
(
    stream
    .key_by(lambda t: t["symbol"])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowed_lateness(Time.seconds(30))       # Accept late data up to 30s
    .side_output_late_data(LATE_DATA_TAG)     # Route very late data to side output
    .aggregate(TradeAggregator())
)
```

**Watermark progression**:
```
  Event time:    t=0   t=1   t=2   t=3   t=4   t=5   t=6
  Watermark:     -5s   -4s   -3s   -2s   -1s    0s   +1s
                                                  ^
                                                  |
                         Window [0, 60s) fires here
                         Late data accepted until watermark = 60s + 30s
```

### 7.5 Watermark Strategy Configuration

```python
# services/flink-processor/config/watermark_config.py

from pyflink.common import WatermarkStrategy, Duration


def bounded_watermark_strategy(
    max_out_of_orderness_seconds: int = 5,
    idleness_timeout_minutes: int = 1,
) -> WatermarkStrategy:
    """
    Standard watermark strategy for all trade data streams.

    Args:
        max_out_of_orderness_seconds: Maximum expected delay between
            event time and processing time. Events arriving later
            than this are considered late.
        idleness_timeout_minutes: If a partition produces no events
            for this duration, it is marked idle so it does not
            hold back the global watermark.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(max_out_of_orderness_seconds)
        )
        .with_timestamp_assigner(
            lambda event, _: event["timestamp_ms"]
        )
        .with_idleness(Duration.of_minutes(idleness_timeout_minutes))
    )
```

---

## 8. Implementation Steps

### 8.1 Directory Structure

```
services/flink-processor/
|-- __init__.py
|-- requirements.txt
|-- Dockerfile
|-- README.md
|
|-- conf/
|   |-- flink-conf.yaml                  # Flink cluster configuration
|   |-- log4j.properties                 # Logging configuration
|
|-- config/
|   |-- __init__.py
|   |-- env_config.py                    # StreamExecutionEnvironment setup
|   |-- kafka_config.py                  # Kafka source/sink builders
|   |-- state_config.py                  # State TTL and backend config
|   |-- watermark_config.py              # Watermark strategies
|
|-- common/
|   |-- __init__.py
|   |-- safe_deserializer.py             # Error-tolerant deserialization
|   |-- error_handler.py                 # Base SafeProcessFunction
|   |-- kafka_helpers.py                 # Reusable Kafka source/sink factories
|   |-- schemas.py                       # Avro schema definitions
|   |-- metrics.py                       # Custom metric helpers
|
|-- jobs/
|   |-- __init__.py
|   |-- trade_event_router.py            # Job 1: Route trades by status
|   |-- trade_aggregation.py             # Job 2: Windowed aggregations
|   |-- price_alert_detection.py         # Job 3: Price movement alerts
|   |-- orderbook_analytics.py           # Job 4: Order book metrics
|   |-- trade_enrichment.py              # Job 5: Reference data enrichment
|   |-- job_runner.py                    # CLI entrypoint to launch any job
|
|-- tests/
|   |-- __init__.py
|   |-- conftest.py                      # Shared fixtures (mini Flink env, mock Kafka)
|   |-- test_trade_router.py             # Unit tests for routing logic
|   |-- test_trade_aggregation.py        # Unit tests for aggregation
|   |-- test_price_alerts.py             # Unit tests for alert detection
|   |-- test_orderbook_analytics.py      # Unit tests for order book metrics
|   |-- test_trade_enrichment.py         # Unit tests for enrichment
|   |-- test_safe_deserializer.py        # Unit tests for error handling
|   |-- integration/
|   |   |-- __init__.py
|   |   |-- test_end_to_end.py           # Full pipeline integration test
|   |   |-- test_exactly_once.py         # Exactly-once delivery verification
|   |   |-- docker-compose.test.yml      # Test infrastructure (Kafka, Flink)
|
|-- data/
|   |-- reference/
|   |   |-- symbols.parquet              # Symbol reference data
|   |   |-- symbols.csv                  # Human-readable reference data
|
services/analytics/
|-- __init__.py
|-- duckdb_analytics.py                  # DuckDB ad-hoc analytics engine
|-- requirements.txt
|-- tests/
|   |-- test_duckdb_analytics.py         # Unit tests for DuckDB queries
```

### 8.2 Ordered Implementation Plan

The implementation follows a dependency-ordered sequence. Each step builds on the previous one.

| Step | File(s)                                            | Description                                                    | Depends On |
|------|----------------------------------------------------|----------------------------------------------------------------|------------|
| 1    | `conf/flink-conf.yaml`                             | Flink cluster configuration                                    | -          |
| 2    | `Dockerfile`                                       | Container image with PyFlink and dependencies                  | Step 1     |
| 3    | `requirements.txt`                                 | Pin all Python dependencies                                    | -          |
| 4    | `config/env_config.py`                             | StreamExecutionEnvironment factory with checkpointing          | Step 1     |
| 5    | `config/kafka_config.py`                           | Kafka connection settings and topic constants                  | -          |
| 6    | `config/state_config.py`                           | State TTL configuration presets                                | -          |
| 7    | `config/watermark_config.py`                       | Watermark strategy factory                                     | -          |
| 8    | `common/schemas.py`                                | Avro schema definitions for all event types                    | -          |
| 9    | `common/kafka_helpers.py`                          | Reusable Kafka source and sink factory functions               | Steps 5, 8|
| 10   | `common/safe_deserializer.py`                      | Error-tolerant JSON/Avro deserialization                       | Step 8     |
| 11   | `common/error_handler.py`                          | Base SafeProcessFunction with side output errors               | -          |
| 12   | `common/metrics.py`                                | Custom Flink metric helpers                                    | -          |
| 13   | `jobs/trade_event_router.py`                       | Job 1: Trade routing by status                                 | Steps 4-12 |
| 14   | `jobs/trade_aggregation.py`                        | Job 2: Windowed trade aggregation                              | Steps 4-12 |
| 15   | `jobs/price_alert_detection.py`                    | Job 3: Price movement detection                                | Steps 4-12 |
| 16   | `jobs/orderbook_analytics.py`                      | Job 4: Order book metrics                                      | Steps 4-12 |
| 17   | `data/reference/symbols.csv`                       | Reference data (seed file)                                     | -          |
| 18   | `jobs/trade_enrichment.py`                         | Job 5: Trade enrichment with reference data                    | Steps 4-12, 17 |
| 19   | `jobs/job_runner.py`                               | CLI entrypoint to select and launch jobs                       | Steps 13-18|
| 20   | `services/analytics/duckdb_analytics.py`           | DuckDB ad-hoc analytics queries                                | Step 17    |
| 21   | `tests/conftest.py`                                | Shared test fixtures                                           | Steps 4-12 |
| 22   | `tests/test_trade_router.py`                       | Unit tests for Job 1                                           | Step 13    |
| 23   | `tests/test_trade_aggregation.py`                  | Unit tests for Job 2                                           | Step 14    |
| 24   | `tests/test_price_alerts.py`                       | Unit tests for Job 3                                           | Step 15    |
| 25   | `tests/test_orderbook_analytics.py`                | Unit tests for Job 4                                           | Step 16    |
| 26   | `tests/test_trade_enrichment.py`                   | Unit tests for Job 5                                           | Step 18    |
| 27   | `tests/test_safe_deserializer.py`                  | Unit tests for error handling                                  | Step 10    |
| 28   | `tests/integration/docker-compose.test.yml`        | Test infrastructure definition                                 | Step 2     |
| 29   | `tests/integration/test_end_to_end.py`             | Full pipeline integration test                                 | Steps 13-18|
| 30   | `tests/integration/test_exactly_once.py`           | Exactly-once delivery verification                             | Step 29    |

---

## 9. Testing Strategy

### 9.1 Unit Testing Flink Operators

Unit tests validate individual processing functions in isolation, without a running Flink cluster or Kafka broker. Each operator is tested by feeding it synthetic input records and asserting on the output.

**Framework**: `pytest` with `pyflink.testing` utilities.

```python
# services/flink-processor/tests/conftest.py

import pytest
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


@pytest.fixture(scope="session")
def mini_flink_env():
    """Create a minimal Flink environment for unit testing."""
    config = Configuration()
    config.set_string("execution.runtime-mode", "BATCH")  # Deterministic for tests
    config.set_integer("parallelism.default", 1)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    return env


@pytest.fixture
def sample_trade():
    """A representative trade event for testing."""
    return {
        "trade_id": "T-001",
        "order_id": "O-001",
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": 100.0,
        "price": 185.50,
        "status": "FILLED",
        "exchange": "NASDAQ",
        "trader_id": "TR-001",
        "account_id": "ACC-001",
        "timestamp_ms": 1700000000000,
        "event_type": "TRADE",
    }


@pytest.fixture
def sample_trades_batch():
    """A batch of trades for aggregation testing."""
    base_ts = 1700000000000
    return [
        {"symbol": "AAPL", "price": 185.0, "quantity": 100, "side": "BUY",
         "status": "FILLED", "timestamp_ms": base_ts},
        {"symbol": "AAPL", "price": 186.0, "quantity": 200, "side": "SELL",
         "status": "FILLED", "timestamp_ms": base_ts + 10000},
        {"symbol": "AAPL", "price": 185.5, "quantity": 150, "side": "BUY",
         "status": "FILLED", "timestamp_ms": base_ts + 20000},
        {"symbol": "GOOG", "price": 140.0, "quantity": 50, "side": "BUY",
         "status": "FILLED", "timestamp_ms": base_ts + 5000},
    ]
```

**Example unit test for the Trade Router**:
```python
# services/flink-processor/tests/test_trade_router.py

import pytest
import json
from jobs.trade_event_router import TradeRouterFunction


class TestTradeRouter:
    """Unit tests for the Trade Event Router processing function."""

    def test_new_order_routes_to_main_output(self, sample_trade):
        sample_trade["status"] = "NEW"
        router = TradeRouterFunction()
        results = list(router.process_element(json.dumps(sample_trade), mock_ctx()))
        assert len(results) == 1
        assert results[0]["status"] == "NEW"

    def test_filled_order_routes_to_filled_side_output(self, sample_trade):
        sample_trade["status"] = "FILLED"
        router = TradeRouterFunction()
        results = list(router.process_element(json.dumps(sample_trade), mock_ctx()))
        # Verify side output tag is TAG_FILLED
        assert results[0][0].tag_id == "filled"

    def test_cancelled_order_routes_correctly(self, sample_trade):
        sample_trade["status"] = "CANCELLED"
        router = TradeRouterFunction()
        results = list(router.process_element(json.dumps(sample_trade), mock_ctx()))
        assert results[0][0].tag_id == "cancelled"

    def test_unknown_status_routes_to_dlq(self, sample_trade):
        sample_trade["status"] = "UNKNOWN_STATUS"
        router = TradeRouterFunction()
        results = list(router.process_element(json.dumps(sample_trade), mock_ctx()))
        assert results[0][0].tag_id == "dlq"

    def test_malformed_json_routes_to_dlq(self):
        router = TradeRouterFunction()
        results = list(router.process_element("not-valid-json{", mock_ctx()))
        assert results[0][0].tag_id == "dlq"

    def test_partially_filled_routes_to_filled(self, sample_trade):
        sample_trade["status"] = "PARTIALLY_FILLED"
        router = TradeRouterFunction()
        results = list(router.process_element(json.dumps(sample_trade), mock_ctx()))
        assert results[0][0].tag_id == "filled"
```

**Example unit test for the Aggregator**:
```python
# services/flink-processor/tests/test_trade_aggregation.py

import pytest
from jobs.trade_aggregation import TradeAggregator


class TestTradeAggregator:
    """Unit tests for the trade aggregation accumulator logic."""

    def test_single_trade_aggregation(self):
        agg = TradeAggregator()
        acc = agg.create_accumulator()
        trade = {"price": 100.0, "quantity": 50.0, "side": "BUY", "timestamp_ms": 1000}
        acc = agg.add(trade, acc)
        result = agg.get_result(acc)

        assert result["total_volume"] == 50.0
        assert result["vwap"] == 100.0
        assert result["trade_count"] == 1
        assert result["buy_count"] == 1
        assert result["sell_count"] == 0

    def test_vwap_calculation(self):
        agg = TradeAggregator()
        acc = agg.create_accumulator()
        acc = agg.add({"price": 100.0, "quantity": 100, "side": "BUY", "timestamp_ms": 1}, acc)
        acc = agg.add({"price": 200.0, "quantity": 100, "side": "SELL", "timestamp_ms": 2}, acc)
        result = agg.get_result(acc)

        # VWAP = (100*100 + 200*100) / (100+100) = 150.0
        assert result["vwap"] == 150.0

    def test_buy_sell_ratio(self):
        agg = TradeAggregator()
        acc = agg.create_accumulator()
        for _ in range(3):
            acc = agg.add({"price": 100, "quantity": 10, "side": "BUY", "timestamp_ms": 1}, acc)
        acc = agg.add({"price": 100, "quantity": 10, "side": "SELL", "timestamp_ms": 2}, acc)
        result = agg.get_result(acc)

        assert result["buy_sell_ratio"] == 3.0

    def test_merge_accumulators(self):
        agg = TradeAggregator()
        acc_a = agg.create_accumulator()
        acc_b = agg.create_accumulator()
        acc_a = agg.add({"price": 100, "quantity": 50, "side": "BUY", "timestamp_ms": 1}, acc_a)
        acc_b = agg.add({"price": 200, "quantity": 50, "side": "SELL", "timestamp_ms": 2}, acc_b)
        merged = agg.merge(acc_a, acc_b)
        result = agg.get_result(merged)

        assert result["trade_count"] == 2
        assert result["total_volume"] == 100.0

    def test_high_low_prices(self):
        agg = TradeAggregator()
        acc = agg.create_accumulator()
        for price in [105.0, 98.0, 110.0, 102.0]:
            acc = agg.add({"price": price, "quantity": 10, "side": "BUY", "timestamp_ms": 1}, acc)
        result = agg.get_result(acc)

        assert result["high_price"] == 110.0
        assert result["low_price"] == 98.0
```

### 9.2 Integration Testing with Embedded Kafka

Integration tests run the full pipeline with real Kafka and Flink components inside Docker containers. They verify end-to-end data flow, topic routing, and exactly-once semantics.

```python
# services/flink-processor/tests/integration/test_end_to_end.py

import pytest
import json
import time
from kafka import KafkaProducer, KafkaConsumer


@pytest.fixture(scope="module")
def kafka_setup():
    """Start Kafka and Flink via docker-compose for integration tests."""
    # docker-compose up is handled by CI or a pytest plugin
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    yield producer
    producer.close()


class TestEndToEndPipeline:
    """Integration tests that verify the full streaming pipeline."""

    def test_trade_routing_end_to_end(self, kafka_setup):
        """Publish a trade to raw.trades and verify it appears on the correct output topic."""
        producer = kafka_setup

        trade = {
            "trade_id": "T-INT-001",
            "order_id": "O-INT-001",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 100,
            "price": 185.50,
            "status": "FILLED",
            "exchange": "NASDAQ",
            "trader_id": "TR-001",
            "account_id": "ACC-001",
            "timestamp_ms": int(time.time() * 1000),
            "event_type": "TRADE",
        }

        producer.send("raw.trades", trade)
        producer.flush()

        # Consume from processed.trades.filled
        consumer = KafkaConsumer(
            "processed.trades.filled",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=30000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if msg.value.get("trade_id") == "T-INT-001":
                break

        consumer.close()
        assert any(m["trade_id"] == "T-INT-001" for m in messages)

    def test_aggregation_produces_output(self, kafka_setup):
        """Publish multiple trades and verify aggregation output."""
        producer = kafka_setup
        base_ts = int(time.time() * 1000)

        for i in range(10):
            producer.send("raw.trades", {
                "trade_id": f"T-AGG-{i:03d}",
                "order_id": f"O-AGG-{i:03d}",
                "symbol": "MSFT",
                "side": "BUY" if i % 2 == 0 else "SELL",
                "quantity": 100 + i * 10,
                "price": 380.0 + i * 0.5,
                "status": "FILLED",
                "exchange": "NASDAQ",
                "trader_id": "TR-001",
                "account_id": "ACC-001",
                "timestamp_ms": base_ts + i * 1000,
                "event_type": "TRADE",
            })
        producer.flush()

        consumer = KafkaConsumer(
            "analytics.trade-aggregates",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=120000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        aggregates = []
        for msg in consumer:
            aggregates.append(msg.value)
            if msg.value.get("symbol") == "MSFT":
                break

        consumer.close()
        assert len(aggregates) > 0
        msft_agg = [a for a in aggregates if a["symbol"] == "MSFT"]
        assert msft_agg[0]["trade_count"] > 0
```

### 9.3 Performance Benchmarks

| Metric                     | Target              | Measurement Method                              |
|----------------------------|---------------------|-------------------------------------------------|
| End-to-end latency         | < 500ms (p99)       | Timestamp diff: event_time to sink_time         |
| Throughput (Trade Router)  | > 50,000 events/sec | Flink metrics: numRecordsOutPerSecond           |
| Throughput (Aggregation)   | > 20,000 events/sec | Flink metrics: numRecordsOutPerSecond           |
| Checkpoint duration        | < 10 seconds        | Flink metrics: lastCheckpointDuration           |
| State size per symbol      | < 1 KB              | RocksDB metrics: estimate-live-data-size        |
| Recovery time              | < 30 seconds        | Time from failure to first output after restart |

**Benchmark test harness**:
```python
# services/flink-processor/tests/integration/benchmark.py

"""
Performance benchmark for the Flink streaming pipeline.
Generates a high volume of synthetic trades and measures
throughput and latency.
"""

import time
import json
from kafka import KafkaProducer, KafkaConsumer
from concurrent.futures import ThreadPoolExecutor

EVENTS_TO_PRODUCE = 100_000
BATCH_SIZE = 1000


def run_throughput_benchmark():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        batch_size=16384 * 4,
    )

    start = time.monotonic()
    for i in range(EVENTS_TO_PRODUCE):
        producer.send("raw.trades", {
            "trade_id": f"BENCH-{i}",
            "order_id": f"BENCH-O-{i}",
            "symbol": f"SYM-{i % 100:03d}",
            "side": "BUY" if i % 2 == 0 else "SELL",
            "quantity": 100.0,
            "price": 100.0 + (i % 50),
            "status": "FILLED",
            "exchange": "NASDAQ",
            "trader_id": "TR-BENCH",
            "account_id": "ACC-BENCH",
            "timestamp_ms": int(time.time() * 1000),
            "event_type": "TRADE",
        })

        if (i + 1) % BATCH_SIZE == 0:
            producer.flush()

    producer.flush()
    elapsed = time.monotonic() - start
    print(f"Produced {EVENTS_TO_PRODUCE} events in {elapsed:.2f}s")
    print(f"Throughput: {EVENTS_TO_PRODUCE / elapsed:,.0f} events/sec")
```

---

## 10. Monitoring

### 10.1 Metrics Architecture

```
  Flink TaskManagers                Prometheus                 Grafana
  +------------------+            +-----------+           +-------------+
  | Prometheus        |  scrape   | Time      |  query    | Dashboards  |
  | Reporter          |---------->| Series    |---------->|             |
  | (port 9249)       |           | Storage   |           | - Throughput|
  +------------------+            +-----------+           | - Latency   |
                                                          | - State     |
  Flink JobManager                                        | - Errors    |
  +------------------+                                    +-------------+
  | REST API          |
  | (port 8081)       |
  +------------------+
```

### 10.2 Prometheus Configuration

File: `infrastructure/monitoring/prometheus/flink-targets.yml`

```yaml
- targets:
    - "flink-jobmanager:9249"
    - "flink-taskmanager-1:9249"
    - "flink-taskmanager-2:9249"
  labels:
    service: "flink"
    environment: "local"
```

File: `infrastructure/monitoring/prometheus/prometheus.yml` (scrape config snippet)

```yaml
scrape_configs:
  - job_name: "flink"
    scrape_interval: 15s
    file_sd_configs:
      - files:
          - "flink-targets.yml"
    metrics_path: /
```

### 10.3 Key Metrics to Monitor

#### Throughput Metrics

| Metric Name                                          | Description                          | Alert Threshold        |
|------------------------------------------------------|--------------------------------------|------------------------|
| `flink_taskmanager_job_task_numRecordsInPerSecond`   | Records consumed per second          | < 100 for > 5 min     |
| `flink_taskmanager_job_task_numRecordsOutPerSecond`  | Records produced per second          | < 100 for > 5 min     |
| `flink_taskmanager_job_task_numBytesInPerSecond`     | Bytes consumed per second            | Trend monitoring       |
| `flink_taskmanager_job_task_numBytesOutPerSecond`    | Bytes produced per second            | Trend monitoring       |

#### Latency Metrics

| Metric Name                                          | Description                          | Alert Threshold        |
|------------------------------------------------------|--------------------------------------|------------------------|
| `flink_taskmanager_job_latency_source_id_*`          | Source-to-operator latency histogram | p99 > 1s              |
| Custom: `trade_processing_latency_ms`                | Event time to output time            | p99 > 500ms           |

#### Checkpoint Metrics

| Metric Name                                          | Description                          | Alert Threshold        |
|------------------------------------------------------|--------------------------------------|------------------------|
| `flink_jobmanager_job_lastCheckpointDuration`        | Duration of last checkpoint          | > 30s                 |
| `flink_jobmanager_job_lastCheckpointSize`            | Size of last checkpoint              | > 1 GB                |
| `flink_jobmanager_job_numberOfCompletedCheckpoints`  | Total successful checkpoints         | No increase for > 5m  |
| `flink_jobmanager_job_numberOfFailedCheckpoints`     | Total failed checkpoints             | > 0 in last 10m       |

#### Backpressure Metrics

| Metric Name                                          | Description                          | Alert Threshold        |
|------------------------------------------------------|--------------------------------------|------------------------|
| `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | Time spent backpressured per second | > 500ms/s          |
| `flink_taskmanager_job_task_busyTimeMsPerSecond`     | Time spent processing per second     | Capacity planning     |
| `flink_taskmanager_job_task_idleTimeMsPerSecond`     | Time spent idle per second           | Scaling indicator     |

#### State and Memory Metrics

| Metric Name                                          | Description                          | Alert Threshold        |
|------------------------------------------------------|--------------------------------------|------------------------|
| `flink_taskmanager_Status_JVM_Memory_Heap_Used`      | JVM heap usage                       | > 80%                 |
| `flink_taskmanager_job_task_Shuffle_Netty_usedMemorySegments` | Network buffer usage        | > 90%                 |
| Custom: `rocksdb_estimate_live_data_size`            | RocksDB state size                   | > 5 GB                |

#### Custom Application Metrics

| Metric Name                       | Description                          | Alert Threshold        |
|-----------------------------------|--------------------------------------|------------------------|
| `deserialization_errors_total`    | Count of deserialization failures    | > 10 in 1 min         |
| `processing_errors_total`         | Count of processing exceptions       | > 5 in 1 min          |
| `late_events_total`               | Count of late-arriving events        | > 1% of total         |
| `dlq_events_total`                | Events sent to dead letter topics    | > 0 in 5 min          |
| `price_alerts_fired_total`        | Number of price alerts generated     | Informational          |

### 10.4 Grafana Dashboard Panels

File: `infrastructure/monitoring/grafana/dashboards/flink-overview.json` (conceptual layout)

```
+---------------------------------------------------------------+
|                    Flink Pipeline Overview                      |
+---------------------------------------------------------------+
| [Records In/sec]  [Records Out/sec]  [Active Jobs]  [Uptime]  |
+---------------------------------------------------------------+
|                                                                 |
|  +----------------------------+  +----------------------------+ |
|  | Throughput Over Time       |  | End-to-End Latency (p99)  | |
|  | (line chart, per job)      |  | (line chart, per job)     | |
|  +----------------------------+  +----------------------------+ |
|                                                                 |
|  +----------------------------+  +----------------------------+ |
|  | Checkpoint Duration        |  | Backpressure %            | |
|  | (line chart + threshold)   |  | (heatmap, per operator)   | |
|  +----------------------------+  +----------------------------+ |
|                                                                 |
|  +----------------------------+  +----------------------------+ |
|  | JVM Heap Usage             |  | State Size (RocksDB)      | |
|  | (gauge, per TaskManager)   |  | (line chart, per job)     | |
|  +----------------------------+  +----------------------------+ |
|                                                                 |
|  +----------------------------+  +----------------------------+ |
|  | Error Rate (DLQ events)    |  | Price Alerts Fired        | |
|  | (bar chart + alert line)   |  | (counter, by severity)    | |
|  +----------------------------+  +----------------------------+ |
+---------------------------------------------------------------+
```

### 10.5 Alerting Rules

File: `infrastructure/monitoring/prometheus/flink-alerts.yml`

```yaml
groups:
  - name: flink_pipeline_alerts
    rules:
      - alert: FlinkJobNotRunning
        expr: flink_jobmanager_numRunningJobs == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "No Flink jobs are running"
          description: "Expected at least 1 running job but found 0 for the last 2 minutes."

      - alert: FlinkHighCheckpointDuration
        expr: flink_jobmanager_job_lastCheckpointDuration > 30000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink checkpoint duration exceeds 30 seconds"

      - alert: FlinkCheckpointsFailing
        expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 0
        labels:
          severity: warning
        annotations:
          summary: "Flink checkpoints are failing"

      - alert: FlinkHighBackpressure
        expr: flink_taskmanager_job_task_backPressuredTimeMsPerSecond > 500
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "Flink operator experiencing sustained backpressure"

      - alert: FlinkThroughputDrop
        expr: flink_taskmanager_job_task_numRecordsInPerSecond < 100
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink input throughput has dropped below 100 records/sec"

      - alert: FlinkHighErrorRate
        expr: rate(deserialization_errors_total[5m]) > 0.1
        labels:
          severity: warning
        annotations:
          summary: "Elevated deserialization error rate"

      - alert: FlinkHighHeapUsage
        expr: >
          flink_taskmanager_Status_JVM_Memory_Heap_Used
          / flink_taskmanager_Status_JVM_Memory_Heap_Max > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink TaskManager heap usage above 85%"
```

---

## Appendix A: Data Flow Summary

```
                          +-------------------+
                          |   WebSocket       |
                          |   Trade Generator |
                          +---------+---------+
                                    |
                                    v
                          +---------+---------+
                          |   Kafka Ingestion |
                          |   raw.trades      |
                          |   raw.marketdata  |
                          |   raw.orderbook   |
                          +---------+---------+
                                    |
                    +---------------+---------------+
                    |               |               |
                    v               v               v
            +-------+------+ +-----+------+ +------+-------+
            | Job 1:       | | Job 2:     | | Job 3:       |
            | Trade Router | | Aggregation| | Price Alerts |
            +-------+------+ +-----+------+ +------+-------+
                    |               |               |
       +------+----+---+---+       v               v
       v      v    v       v   analytics.    alerts.price-
   new   filled cancelled rejected  trade-agg   movement
       |
       v                              +-------+--------+
  +----+----------+                   | Job 4:         |
  | Job 5:        |                   | OrderBook      |
  | Enrichment    |                   | Analytics      |
  +----+----------+                   +-------+--------+
       |                                      |
       v                                      v
  enriched.trades                     analytics.orderbook
       |                                      |
       +-------------------+------------------+
                           |
                           v
                  +--------+--------+
                  | Parquet/Iceberg |
                  | Storage Layer   |
                  +--------+--------+
                           |
                           v
                  +--------+--------+
                  | DuckDB Ad-Hoc   |
                  | Analytics       |
                  +-----------------+
```

## Appendix B: Configuration Quick Reference

| Parameter                          | Value                | Rationale                                    |
|------------------------------------|----------------------|----------------------------------------------|
| Checkpoint interval                | 30 seconds           | Balance latency vs checkpoint overhead       |
| Checkpoint mode                    | EXACTLY_ONCE         | Required for end-to-end guarantees           |
| State backend                      | RocksDB              | Supports large state beyond heap memory      |
| Incremental checkpoints            | Enabled              | Reduces checkpoint size for large state      |
| Default parallelism                | 4                    | Matches TaskManager slot count               |
| Restart attempts                   | 3                    | Recovers from transient failures             |
| Restart delay                      | 10 seconds           | Gives dependent services time to recover     |
| Watermark out-of-orderness         | 5 seconds            | Tolerates moderate event-time skew           |
| Allowed lateness (windows)         | 30 seconds           | Accepts late arrivals within reason          |
| State TTL (price alerts)           | 10 minutes           | Matches detection window + buffer            |
| Kafka raw topic partitions         | 8                    | Enables parallel consumption                 |
| Kafka processed topic partitions   | 4                    | Matches Flink parallelism                    |
| Kafka raw topic retention          | 7 days               | Replay window for reprocessing               |
