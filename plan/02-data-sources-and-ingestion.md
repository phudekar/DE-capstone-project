# 02 - Data Sources & Ingestion: Implementation Plan

> **Component**: Phase 1 -- Data Sources & Ingestion
> **Parent Document**: [High-Level Requirement](./high-level-requirement.md)
> **Last Updated**: 2026-02-18
> **Status**: Draft

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Trade Data Generator (Console Application)](#2-trade-data-generator-console-application)
3. [WebSocket to Kafka Bridge](#3-websocket-to-kafka-bridge)
4. [Kafka Cluster Setup](#4-kafka-cluster-setup)
5. [Avro Schema Definitions](#5-avro-schema-definitions)
6. [Data Contracts](#6-data-contracts)
7. [Implementation Steps](#7-implementation-steps)
8. [Testing Strategy](#8-testing-strategy)
9. [Configuration](#9-configuration)
10. [Appendix](#10-appendix)

---

## 1. Architecture Overview

```
+---------------------+         WebSocket          +---------------------+
|                     |  (ws://localhost:8765)      |                     |
|  Trade Data         | =========================> |  WebSocket-Kafka    |
|  Generator          |   JSON messages            |  Bridge             |
|  (Python Console)   |   (schema-versioned)       |  (Kafka Producer)   |
|                     |                            |                     |
+---------------------+                            +---------------------+
        |                                                    |
        | Generates:                                         | Produces to:
        |  - Trade events                                    |  - raw.trades
        |  - OrderBook snapshots                             |  - raw.orderbook
        |  - MarketData ticks                                |  - raw.marketdata
        |                                                    |
        v                                                    v
  CLI Interface                                   +---------------------+
  (argparse)                                      |  Kafka Cluster      |
  --rate, --symbols,                              |  (3 brokers, KRaft) |
  --mode, --port                                  |                     |
                                                  |  + Schema Registry  |
                                                  |  + Kafka UI         |
                                                  +---------------------+
```

**Data Flow Summary**:

1. The Trade Data Generator produces realistic stock market events (trades, order book updates, market data ticks) and serves them over a WebSocket server.
2. The WebSocket-Kafka Bridge connects as a WebSocket client, deserializes JSON messages, validates them, serializes to Avro, and produces them to the appropriate Kafka topic.
3. Kafka stores the raw events durably across a 3-broker cluster with schema enforcement via Confluent Schema Registry.

---

## 2. Trade Data Generator (Console Application)

### 2.1 Application Design

**Language**: Python 3.11+
**Async Framework**: `asyncio` (native)
**WebSocket Library**: `websockets`
**CLI**: `argparse`
**Package Structure**: Standard Python package with `pyproject.toml`

```
services/trade-generator/
|-- pyproject.toml
|-- README.md
|-- src/
|   |-- trade_generator/
|       |-- __init__.py
|       |-- __main__.py              # Entry point (python -m trade_generator)
|       |-- cli.py                   # CLI argument parsing
|       |-- server.py                # WebSocket server
|       |-- engine/
|       |   |-- __init__.py
|       |   |-- trade_engine.py      # Core trade generation logic
|       |   |-- orderbook_engine.py  # Order book simulation
|       |   |-- market_data_engine.py# Market data generation
|       |   |-- price_simulator.py   # Realistic price movement (GBM)
|       |   |-- symbol_registry.py   # Stock symbol metadata
|       |-- models/
|       |   |-- __init__.py
|       |   |-- trade.py             # Trade dataclass
|       |   |-- orderbook.py         # OrderBook dataclass
|       |   |-- market_data.py       # MarketData dataclass
|       |   |-- enums.py             # Shared enums
|       |-- serialization/
|       |   |-- __init__.py
|       |   |-- json_serializer.py   # JSON encoding with schema version
|       |-- config/
|       |   |-- __init__.py
|       |   |-- settings.py          # Pydantic Settings
|       |   |-- symbols.json         # Symbol reference data
|-- tests/
|   |-- __init__.py
|   |-- test_trade_engine.py
|   |-- test_orderbook_engine.py
|   |-- test_market_data_engine.py
|   |-- test_price_simulator.py
|   |-- test_server.py
|   |-- test_cli.py
|   |-- conftest.py
```

### 2.2 Data Models

#### 2.2.1 Enums

```python
# models/enums.py
from enum import Enum

class TradeType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"

class TradeStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class Exchange(str, Enum):
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"
    AMEX = "AMEX"
    ARCA = "ARCA"
    BATS = "BATS"
```

#### 2.2.2 Trade Model

```python
# models/trade.py
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4
from .enums import TradeType, OrderType, TradeStatus, Exchange

@dataclass(frozen=True)
class Trade:
    trade_id: UUID = field(default_factory=uuid4)
    symbol: str = ""
    trade_type: TradeType = TradeType.BUY
    order_type: OrderType = OrderType.MARKET
    price: Decimal = Decimal("0.00")
    quantity: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.utcnow())
    exchange: Exchange = Exchange.NYSE
    trader_id: str = ""
    account_id: str = ""
    status: TradeStatus = TradeStatus.NEW

    # Metadata fields for lineage
    schema_version: int = 1
    source: str = "trade-generator"
    correlation_id: str = ""  # Links orders to fills
```

#### 2.2.3 OrderBook Model

```python
# models/orderbook.py
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

@dataclass(frozen=True)
class OrderBook:
    symbol: str = ""
    bid_price: Decimal = Decimal("0.00")
    ask_price: Decimal = Decimal("0.00")
    bid_size: int = 0
    ask_size: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.utcnow())
    spread: Decimal = Decimal("0.00")  # Computed: ask_price - bid_price
    mid_price: Decimal = Decimal("0.00")  # Computed: (bid + ask) / 2
    exchange: str = ""
    schema_version: int = 1
    source: str = "trade-generator"
```

#### 2.2.4 MarketData Model

```python
# models/market_data.py
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

@dataclass(frozen=True)
class MarketData:
    symbol: str = ""
    open: Decimal = Decimal("0.00")
    high: Decimal = Decimal("0.00")
    low: Decimal = Decimal("0.00")
    close: Decimal = Decimal("0.00")
    volume: int = 0
    vwap: Decimal = Decimal("0.00")  # Volume Weighted Average Price
    timestamp: datetime = field(default_factory=lambda: datetime.utcnow())
    interval: str = "1m"  # Candle interval: 1m, 5m, 15m, 1h, 1d
    exchange: str = ""
    schema_version: int = 1
    source: str = "trade-generator"
```

### 2.3 Realistic Data Generation Strategy

#### 2.3.1 Symbol Registry (~50 Symbols)

The symbol registry will contain reference data for approximately 50 stocks spanning multiple sectors. This data drives realistic price ranges and behaviors.

```json
// config/symbols.json (excerpt)
{
  "symbols": [
    {
      "symbol": "AAPL",
      "name": "Apple Inc.",
      "exchange": "NASDAQ",
      "sector": "Technology",
      "base_price": 178.50,
      "volatility": 0.02,
      "avg_daily_volume": 55000000,
      "tick_size": 0.01,
      "lot_size": 100,
      "weight": 0.08
    },
    {
      "symbol": "GOOGL",
      "name": "Alphabet Inc.",
      "exchange": "NASDAQ",
      "sector": "Technology",
      "base_price": 141.80,
      "volatility": 0.025,
      "avg_daily_volume": 22000000,
      "tick_size": 0.01,
      "lot_size": 100,
      "weight": 0.06
    },
    {
      "symbol": "MSFT",
      "name": "Microsoft Corp.",
      "exchange": "NASDAQ",
      "sector": "Technology",
      "base_price": 415.30,
      "volatility": 0.018,
      "avg_daily_volume": 20000000,
      "tick_size": 0.01,
      "lot_size": 100,
      "weight": 0.07
    }
  ]
}
```

Full symbol list will cover: Technology (15), Finance (10), Healthcare (8), Consumer (7), Energy (5), Industrials (5).

The `weight` field determines how frequently a symbol appears in generated trades. Higher-cap stocks like AAPL get more trades.

#### 2.3.2 Price Simulation (Geometric Brownian Motion)

Use GBM to produce realistic intraday price movements:

```
dS = S * (mu * dt + sigma * dW)
```

Where:
- `S` = current price
- `mu` = drift (annualized, typically small for intraday, e.g., 0.0001)
- `sigma` = volatility (from symbol registry, per-stock)
- `dW` = Wiener process increment (`sqrt(dt) * N(0,1)`)
- `dt` = time step (1/num_ticks_per_day)

```python
# engine/price_simulator.py
import numpy as np
from decimal import Decimal

class PriceSimulator:
    """
    Simulates realistic stock price movements using Geometric Brownian Motion.
    Maintains state per symbol for continuous price evolution.
    """

    def __init__(self, seed: int | None = None):
        self.rng = np.random.default_rng(seed)
        self._prices: dict[str, float] = {}  # Current price per symbol

    def initialize_symbol(self, symbol: str, base_price: float) -> None:
        self._prices[symbol] = base_price

    def next_price(self, symbol: str, volatility: float, dt: float = 1/23400) -> Decimal:
        """
        Generate next price tick. dt=1/23400 assumes 6.5hr trading day
        with per-second ticks (6.5 * 3600 = 23400).
        """
        current = self._prices[symbol]
        mu = 0.0001  # Small positive drift
        dW = self.rng.normal(0, np.sqrt(dt))
        new_price = current * np.exp((mu - 0.5 * volatility**2) * dt + volatility * dW)
        self._prices[symbol] = new_price
        return Decimal(str(round(new_price, 2)))

    def get_current_price(self, symbol: str) -> Decimal:
        return Decimal(str(round(self._prices[symbol], 2)))
```

#### 2.3.3 Market Hours Simulation

The generator will simulate realistic trading patterns across the US market day:

| Time Window (ET)     | Period             | Trade Frequency Multiplier |
|----------------------|--------------------|---------------------------|
| 04:00 - 09:30        | Pre-market          | 0.1x                      |
| 09:30 - 09:45        | Opening auction     | 3.0x                      |
| 09:45 - 11:30        | Morning session     | 1.5x                      |
| 11:30 - 13:00        | Midday lull         | 0.6x                      |
| 13:00 - 15:30        | Afternoon session   | 1.0x                      |
| 15:30 - 16:00        | Closing auction     | 2.5x                      |
| 16:00 - 20:00        | After-hours         | 0.1x                      |

Implementation approach:

```python
# engine/trade_engine.py
from datetime import datetime, time

MARKET_PERIODS = [
    (time(4, 0), time(9, 30), 0.1, "pre_market"),
    (time(9, 30), time(9, 45), 3.0, "opening_auction"),
    (time(9, 45), time(11, 30), 1.5, "morning"),
    (time(11, 30), time(13, 0), 0.6, "midday"),
    (time(13, 0), time(15, 30), 1.0, "afternoon"),
    (time(15, 30), time(16, 0), 2.5, "closing_auction"),
    (time(16, 0), time(20, 0), 0.1, "after_hours"),
]

def get_frequency_multiplier(current_time: time) -> float:
    for start, end, multiplier, _ in MARKET_PERIODS:
        if start <= current_time < end:
            return multiplier
    return 0.0  # Market closed
```

#### 2.3.4 Correlated Trade Generation (Order Lifecycle)

Trades are not generated in isolation. The engine simulates a realistic order lifecycle:

```
NEW order placed
    |
    v
[50% chance] --> FILLED (single fill, full quantity)
[30% chance] --> PARTIALLY_FILLED --> FILLED (2-3 partial fills)
[10% chance] --> CANCELLED (by trader)
[10% chance] --> REJECTED (invalid price, insufficient balance)
```

Implementation:

```python
# engine/trade_engine.py
class TradeEngine:
    """
    Generates correlated trade events simulating order lifecycles.
    """

    def __init__(self, price_simulator: PriceSimulator, symbols: list[SymbolConfig]):
        self.price_simulator = price_simulator
        self.symbols = symbols
        self._pending_orders: dict[str, PendingOrder] = {}
        self._trader_pool = self._generate_trader_pool(num_traders=200)

    async def generate_trade(self) -> list[Trade]:
        """
        Returns one or more Trade events per invocation.
        May return a new order, a fill for a pending order, or a cancellation.
        """
        events = []

        # First, check if any pending orders should be filled/cancelled
        if self._pending_orders and self._rng.random() < 0.4:
            events.extend(self._resolve_pending_order())

        # Generate a new order
        symbol = self._select_symbol_weighted()
        price = self.price_simulator.next_price(
            symbol.symbol, symbol.volatility
        )
        trade = self._create_new_order(symbol, price)
        events.append(trade)

        # Some orders get immediate fills (MARKET orders)
        if trade.order_type == OrderType.MARKET and trade.status == TradeStatus.NEW:
            fill = self._create_immediate_fill(trade)
            events.append(fill)

        return events

    def _select_symbol_weighted(self) -> SymbolConfig:
        weights = [s.weight for s in self.symbols]
        return self._rng.choice(self.symbols, p=weights / np.sum(weights))

    def _generate_trader_pool(self, num_traders: int) -> list[dict]:
        """Pre-generate a pool of trader_id/account_id pairs."""
        return [
            {
                "trader_id": f"TRD-{i:05d}",
                "account_id": f"ACC-{i // 5:04d}",  # ~5 traders per account
            }
            for i in range(num_traders)
        ]
```

#### 2.3.5 Throughput Modes

| Mode        | Behavior                                                                 |
|-------------|--------------------------------------------------------------------------|
| `normal`    | Steady-state generation at `--rate` trades/sec, modulated by market hours |
| `burst`     | Periodic bursts of 10x--50x normal rate, simulating flash crashes / news  |
| `historical`| Generates a full day of historical data as fast as possible, timestamped retroactively |

Burst mode details:
- Every 30-120 seconds (random), trigger a burst lasting 5-15 seconds
- During burst, increase rate by 10x-50x (configurable)
- Optionally correlate bursts with a single symbol (simulating news events)

### 2.4 WebSocket Server Implementation

```python
# server.py
import asyncio
import json
import logging
from datetime import datetime
from websockets.asyncio.server import serve, ServerConnection

logger = logging.getLogger(__name__)

class TradeWebSocketServer:
    """
    WebSocket server that streams generated trade data to connected clients.
    Supports multiple concurrent clients. Each client receives all events.
    """

    def __init__(
        self,
        trade_engine: TradeEngine,
        orderbook_engine: OrderBookEngine,
        market_data_engine: MarketDataEngine,
        host: str = "0.0.0.0",
        port: int = 8765,
        rate: float = 10.0,
    ):
        self.trade_engine = trade_engine
        self.orderbook_engine = orderbook_engine
        self.market_data_engine = market_data_engine
        self.host = host
        self.port = port
        self.rate = rate
        self.clients: set[ServerConnection] = set()
        self._running = False
        self._stats = {"messages_sent": 0, "clients_connected": 0}

    async def handler(self, websocket: ServerConnection) -> None:
        """Handle a new WebSocket client connection."""
        self.clients.add(websocket)
        self._stats["clients_connected"] = len(self.clients)
        logger.info(f"Client connected. Total clients: {len(self.clients)}")
        try:
            # Keep connection alive; client is read-only
            async for message in websocket:
                # Clients can send control messages (e.g., subscribe to symbols)
                await self._handle_client_message(websocket, message)
        finally:
            self.clients.discard(websocket)
            self._stats["clients_connected"] = len(self.clients)
            logger.info(f"Client disconnected. Total clients: {len(self.clients)}")

    async def broadcast(self, message: str) -> None:
        """Send a message to all connected clients."""
        if not self.clients:
            return
        # Use gather for concurrent sends; filter out failed connections
        results = await asyncio.gather(
            *[client.send(message) for client in self.clients],
            return_exceptions=True,
        )
        for client, result in zip(list(self.clients), results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to send to client: {result}")
                self.clients.discard(client)

    async def _generation_loop(self) -> None:
        """Main loop: generate events at configured rate and broadcast."""
        interval = 1.0 / self.rate
        while self._running:
            start = asyncio.get_event_loop().time()

            # Generate trade events
            trades = await self.trade_engine.generate_trade()
            for trade in trades:
                msg = self._serialize_event("trade", trade)
                await self.broadcast(msg)
                self._stats["messages_sent"] += 1

            # Generate orderbook snapshot (every 5th tick)
            if self._stats["messages_sent"] % 5 == 0:
                orderbook = self.orderbook_engine.generate_snapshot()
                msg = self._serialize_event("orderbook", orderbook)
                await self.broadcast(msg)

            # Generate market data candle (every 60th tick)
            if self._stats["messages_sent"] % 60 == 0:
                market_data = self.market_data_engine.generate_candle()
                msg = self._serialize_event("marketdata", market_data)
                await self.broadcast(msg)

            # Sleep to maintain target rate
            elapsed = asyncio.get_event_loop().time() - start
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    def _serialize_event(self, event_type: str, event: object) -> str:
        """Serialize event to JSON with envelope metadata."""
        return json.dumps({
            "schema_version": 1,
            "event_type": event_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": self._to_dict(event),
        }, default=str)

    async def start(self) -> None:
        """Start the WebSocket server and generation loop."""
        self._running = True
        async with serve(self.handler, self.host, self.port) as server:
            logger.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await self._generation_loop()

    async def stop(self) -> None:
        """Gracefully stop the server."""
        self._running = False
```

### 2.5 Message Format (JSON with Schema Versioning)

Every message sent over the WebSocket uses an envelope format:

```json
{
  "schema_version": 1,
  "event_type": "trade",
  "timestamp": "2026-02-18T14:30:00.123456Z",
  "payload": {
    "trade_id": "550e8400-e29b-41d4-a716-446655440000",
    "symbol": "AAPL",
    "trade_type": "BUY",
    "order_type": "MARKET",
    "price": "178.52",
    "quantity": 100,
    "timestamp": "2026-02-18T14:30:00.123456Z",
    "exchange": "NASDAQ",
    "trader_id": "TRD-00042",
    "account_id": "ACC-0008",
    "status": "NEW",
    "correlation_id": "corr-abc123"
  }
}
```

Schema versioning rules:
- `schema_version` is an integer, incremented on breaking changes.
- Non-breaking additions (new optional fields) do not increment the version.
- The consumer uses `schema_version` to select the correct deserialization path.

### 2.6 CLI Interface

```python
# cli.py
import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="trade-generator",
        description="Generate realistic stock market trade data and stream via WebSocket",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Trades per second (default: 10). Actual rate modulated by market hour simulation.",
    )
    parser.add_argument(
        "--symbols",
        type=int,
        default=50,
        help="Number of stock symbols to simulate (default: 50, max: 50).",
    )
    parser.add_argument(
        "--mode",
        choices=["normal", "burst", "historical"],
        default="normal",
        help=(
            "Generation mode. "
            "'normal': steady-state real-time. "
            "'burst': periodic high-volume spikes. "
            "'historical': generate a full day backfill as fast as possible."
        ),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="WebSocket server port (default: 8765).",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="WebSocket server bind address (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible data generation.",
    )
    parser.add_argument(
        "--burst-multiplier",
        type=float,
        default=20.0,
        help="Rate multiplier during burst events (default: 20x).",
    )
    parser.add_argument(
        "--burst-interval",
        type=int,
        default=60,
        help="Average seconds between burst events (default: 60).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO).",
    )

    return parser.parse_args()
```

### 2.7 Dependencies

```toml
# pyproject.toml
[project]
name = "trade-generator"
version = "0.1.0"
description = "Stock market trade data generator with WebSocket streaming"
requires-python = ">=3.11"

dependencies = [
    "websockets>=13.0,<14.0",
    "numpy>=1.26,<2.0",
    "pydantic>=2.5,<3.0",
    "pydantic-settings>=2.1,<3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.0",
    "ruff>=0.3",
    "mypy>=1.8",
]

[project.scripts]
trade-generator = "trade_generator.__main__:main"
```

---

## 3. WebSocket to Kafka Bridge

### 3.1 Application Design

**Language**: Python 3.11+
**Kafka Client**: `confluent-kafka` (librdkafka-based, high performance)
**WebSocket Client**: `websockets`
**Schema Registry**: `confluent-kafka[avro]`
**Metrics**: `prometheus-client`

```
services/kafka-bridge/
|-- pyproject.toml
|-- README.md
|-- src/
|   |-- kafka_bridge/
|       |-- __init__.py
|       |-- __main__.py              # Entry point
|       |-- cli.py                   # CLI argument parsing
|       |-- bridge.py                # Main bridge orchestrator
|       |-- websocket_client.py      # WebSocket consumer
|       |-- kafka_producer.py        # Kafka producer wrapper
|       |-- message_router.py        # Routes messages to correct topics
|       |-- serialization/
|       |   |-- __init__.py
|       |   |-- avro_serializer.py   # Avro serialization with Schema Registry
|       |-- validation/
|       |   |-- __init__.py
|       |   |-- message_validator.py # JSON schema validation
|       |-- metrics/
|       |   |-- __init__.py
|       |   |-- prometheus.py        # Prometheus metrics
|       |-- error_handling/
|       |   |-- __init__.py
|       |   |-- dlq.py              # Dead letter queue handler
|       |   |-- retry.py            # Retry with backoff
|       |-- config/
|       |   |-- __init__.py
|       |   |-- settings.py
|-- tests/
|   |-- __init__.py
|   |-- test_bridge.py
|   |-- test_websocket_client.py
|   |-- test_kafka_producer.py
|   |-- test_message_router.py
|   |-- conftest.py
```

### 3.2 WebSocket Client

```python
# websocket_client.py
import asyncio
import logging
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

class WebSocketConsumer:
    """
    Connects to the trade generator WebSocket server and yields messages.
    Implements reconnection with exponential backoff.
    """

    def __init__(
        self,
        uri: str = "ws://localhost:8765",
        max_retries: int = 10,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ):
        self.uri = uri
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self._retry_count = 0

    async def connect_and_consume(self, message_callback):
        """
        Connect to WebSocket and invoke callback for each message.
        Automatically reconnects on failure.
        """
        while True:
            try:
                async with connect(self.uri) as websocket:
                    logger.info(f"Connected to {self.uri}")
                    self._retry_count = 0  # Reset on successful connection

                    async for message in websocket:
                        await message_callback(message)

            except ConnectionClosed as e:
                logger.warning(f"Connection closed: {e.code} {e.reason}")
                await self._reconnect_backoff()

            except OSError as e:
                logger.error(f"Connection failed: {e}")
                await self._reconnect_backoff()

    async def _reconnect_backoff(self) -> None:
        """Exponential backoff with jitter."""
        if self._retry_count >= self.max_retries:
            logger.critical("Max retries exceeded. Exiting.")
            raise ConnectionError(f"Failed to connect after {self.max_retries} retries")

        backoff = min(
            self.base_backoff * (2 ** self._retry_count),
            self.max_backoff,
        )
        jitter = backoff * 0.1 * (asyncio.get_event_loop().time() % 1)
        wait_time = backoff + jitter
        self._retry_count += 1

        logger.info(
            f"Reconnecting in {wait_time:.1f}s "
            f"(attempt {self._retry_count}/{self.max_retries})"
        )
        await asyncio.sleep(wait_time)
```

### 3.3 Kafka Producer Configuration

```python
# kafka_producer.py
import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logger = logging.getLogger(__name__)

class KafkaTradeProducer:
    """
    Kafka producer configured for reliable, ordered delivery of trade events.
    Uses Avro serialization with Schema Registry.
    """

    # --- Core Kafka Producer Configuration ---
    PRODUCER_CONFIG = {
        # Broker connection
        "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
        "client.id": "trade-bridge-producer",

        # Durability: wait for all in-sync replicas
        "acks": "all",

        # Idempotent producer: exactly-once semantics within a partition
        "enable.idempotence": True,

        # Retries (idempotent producer requires retries > 0)
        "retries": 2147483647,  # Effectively infinite
        "max.in.flight.requests.per.connection": 5,  # Safe with idempotence

        # Batching for throughput
        "batch.size": 65536,          # 64 KB batch
        "linger.ms": 10,              # Wait up to 10ms to fill batch
        "batch.num.messages": 500,    # Max messages per batch

        # Compression
        "compression.type": "snappy",  # Good balance of speed vs ratio

        # Buffer memory
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 1048576,  # 1 GB

        # Delivery timeout
        "delivery.timeout.ms": 120000,  # 2 minutes
        "request.timeout.ms": 30000,    # 30 seconds

        # Monitoring callbacks
        "statistics.interval.ms": 60000,
    }

    def __init__(self, config_overrides: dict | None = None):
        config = {**self.PRODUCER_CONFIG, **(config_overrides or {})}
        self.producer = Producer(config)

        # Schema Registry client
        self.schema_registry = SchemaRegistryClient({
            "url": "http://localhost:8081",
        })

        # Per-topic Avro serializers (initialized lazily)
        self._serializers: dict[str, AvroSerializer] = {}

        # Metrics
        self._messages_produced = 0
        self._messages_failed = 0

    def produce(
        self,
        topic: str,
        key: str,
        value: dict,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Produce a message to the specified Kafka topic.
        Key is the symbol (for partition ordering).
        Value is serialized to Avro.
        """
        serializer = self._get_serializer(topic)
        serialized_value = serializer(
            value,
            SerializationContext(topic, MessageField.VALUE),
        )

        kafka_headers = [
            (k, v.encode("utf-8")) for k, v in (headers or {}).items()
        ]

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=serialized_value,
            headers=kafka_headers,
            callback=self._delivery_callback,
        )

        # Trigger delivery of buffered messages
        self.producer.poll(0)

    def _delivery_callback(self, err, msg) -> None:
        """Called for each message delivery (success or failure)."""
        if err is not None:
            logger.error(
                f"Delivery failed: topic={msg.topic()}, "
                f"key={msg.key()}, error={err}"
            )
            self._messages_failed += 1
        else:
            self._messages_produced += 1
            logger.debug(
                f"Delivered: topic={msg.topic()}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
            )

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all buffered messages. Returns number of outstanding messages."""
        return self.producer.flush(timeout)
```

### 3.4 Partitioning Strategy

Messages are partitioned **by symbol** to guarantee per-symbol ordering:

```python
# message_router.py
import hashlib

class MessageRouter:
    """Routes messages to the correct Kafka topic and determines the partition key."""

    TOPIC_MAP = {
        "trade": "raw.trades",
        "orderbook": "raw.orderbook",
        "marketdata": "raw.marketdata",
    }

    def route(self, event_type: str, payload: dict) -> tuple[str, str]:
        """
        Returns (topic, partition_key) for a given event.
        Partition key is always the symbol to guarantee ordering.
        """
        topic = self.TOPIC_MAP.get(event_type)
        if topic is None:
            raise ValueError(f"Unknown event type: {event_type}")

        # Use symbol as partition key for ordering guarantees
        partition_key = payload.get("symbol", "UNKNOWN")
        return topic, partition_key
```

Why partition by symbol:
- All trades for AAPL land on the same partition and are therefore **strictly ordered**.
- Downstream consumers (Flink) can process per-symbol state without cross-partition coordination.
- With 50 symbols across 12 partitions, each partition handles roughly 4 symbols -- a balanced distribution.

### 3.5 Error Handling

#### 3.5.1 WebSocket Reconnection

See the `WebSocketConsumer._reconnect_backoff()` method in Section 3.2. Parameters:

| Parameter        | Default | Description                              |
|------------------|---------|------------------------------------------|
| `max_retries`    | 10      | Maximum reconnection attempts            |
| `base_backoff`   | 1.0s    | Initial backoff duration                 |
| `max_backoff`    | 60.0s   | Maximum backoff cap                      |
| Jitter           | 10%     | Random jitter added to prevent thundering herd |

#### 3.5.2 Kafka Delivery Failure Handling

```python
# error_handling/retry.py
class DeliveryRetryHandler:
    """
    Handles Kafka delivery failures with configurable retry policy.
    """

    def __init__(self, max_retries: int = 3, dlq_producer: "DLQProducer" = None):
        self.max_retries = max_retries
        self.dlq_producer = dlq_producer
        self._retry_counts: dict[str, int] = {}  # message_id -> retry count

    def handle_failure(self, message_id: str, topic: str, key: str, value: dict, error: str):
        retries = self._retry_counts.get(message_id, 0)

        if retries < self.max_retries:
            self._retry_counts[message_id] = retries + 1
            logger.warning(
                f"Retrying message {message_id} "
                f"(attempt {retries + 1}/{self.max_retries})"
            )
            return "RETRY"
        else:
            logger.error(f"Message {message_id} exceeded max retries. Sending to DLQ.")
            if self.dlq_producer:
                self.dlq_producer.send_to_dlq(topic, key, value, error)
            del self._retry_counts[message_id]
            return "DLQ"
```

#### 3.5.3 Dead Letter Queue

Messages that fail all retries are sent to a dead letter topic for investigation:

```python
# error_handling/dlq.py
import json
from datetime import datetime

class DLQProducer:
    """
    Sends failed messages to a dead letter queue topic with error metadata.
    DLQ topic: dlq.raw.failed
    """

    DLQ_TOPIC = "dlq.raw.failed"

    def __init__(self, producer: Producer):
        self.producer = producer

    def send_to_dlq(
        self,
        original_topic: str,
        key: str,
        value: dict,
        error: str,
    ) -> None:
        dlq_message = {
            "original_topic": original_topic,
            "original_key": key,
            "original_value": value,
            "error": error,
            "failed_at": datetime.utcnow().isoformat() + "Z",
            "retry_count": 3,
        }
        self.producer.produce(
            topic=self.DLQ_TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(dlq_message).encode("utf-8"),
        )
```

### 3.6 Prometheus Metrics

```python
# metrics/prometheus.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# WebSocket metrics
WS_MESSAGES_RECEIVED = Counter(
    "bridge_ws_messages_received_total",
    "Total messages received from WebSocket",
    ["event_type"],
)
WS_CONNECTION_STATUS = Gauge(
    "bridge_ws_connected",
    "WebSocket connection status (1=connected, 0=disconnected)",
)
WS_RECONNECT_TOTAL = Counter(
    "bridge_ws_reconnects_total",
    "Total WebSocket reconnection attempts",
)

# Kafka metrics
KAFKA_MESSAGES_PRODUCED = Counter(
    "bridge_kafka_messages_produced_total",
    "Total messages successfully produced to Kafka",
    ["topic"],
)
KAFKA_MESSAGES_FAILED = Counter(
    "bridge_kafka_messages_failed_total",
    "Total messages that failed Kafka delivery",
    ["topic"],
)
KAFKA_PRODUCE_LATENCY = Histogram(
    "bridge_kafka_produce_latency_seconds",
    "Kafka produce latency in seconds",
    ["topic"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# DLQ metrics
DLQ_MESSAGES_TOTAL = Counter(
    "bridge_dlq_messages_total",
    "Total messages sent to dead letter queue",
    ["original_topic"],
)

# Validation metrics
VALIDATION_FAILURES = Counter(
    "bridge_validation_failures_total",
    "Total messages that failed validation",
    ["event_type", "reason"],
)

def start_metrics_server(port: int = 9090) -> None:
    """Start Prometheus metrics HTTP server."""
    start_http_server(port)
```

### 3.7 Bridge Dependencies

```toml
# pyproject.toml
[project]
name = "kafka-bridge"
version = "0.1.0"
description = "WebSocket to Kafka bridge for trade data ingestion"
requires-python = ">=3.11"

dependencies = [
    "websockets>=13.0,<14.0",
    "confluent-kafka[avro]>=2.3,<3.0",
    "pydantic>=2.5,<3.0",
    "pydantic-settings>=2.1,<3.0",
    "prometheus-client>=0.20,<1.0",
    "jsonschema>=4.20,<5.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.0",
    "testcontainers[kafka]>=3.7",
    "ruff>=0.3",
    "mypy>=1.8",
]

[project.scripts]
kafka-bridge = "kafka_bridge.__main__:main"
```

---

## 4. Kafka Cluster Setup

### 4.1 Cluster Architecture (KRaft Mode)

We will use **KRaft mode** (Kafka Raft) to eliminate the ZooKeeper dependency. This simplifies operations and is the recommended approach for Kafka 3.7+.

```
+-----------+    +-----------+    +-----------+
| Broker 1  |    | Broker 2  |    | Broker 3  |
| :9092     |    | :9093     |    | :9094     |
| (Ctrl+Brk)|    | (Ctrl+Brk)|    | (Ctrl+Brk)|
+-----------+    +-----------+    +-----------+
      |                |                |
      +--------+-------+-------+--------+
               |               |
        +------+------+ +-----+------+
        | Schema      | | Kafka UI   |
        | Registry    | | :8080      |
        | :8081       | |            |
        +-------------+ +------------+
```

### 4.2 Docker Compose Configuration

```yaml
# infrastructure/docker-compose/docker-compose.kafka.yml
version: "3.8"

services:
  # --- Kafka Broker 1 (Controller + Broker) ---
  kafka-1:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-1
    container_name: kafka-1
    ports:
      - "9092:9092"       # External listener
      - "29092:29092"     # Internal listener
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-1:29092,EXTERNAL://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG_RETENTION_HOURS: 168       # 7 days
      KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1 GB
      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 300000
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_MESSAGE_MAX_BYTES: 1048576     # 1 MB max message
      KAFKA_REPLICA_FETCH_MAX_BYTES: 1048576
    volumes:
      - kafka-1-data:/var/lib/kafka/data
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:29092"]
      interval: 10s
      timeout: 10s
      retries: 5

  # --- Kafka Broker 2 ---
  kafka-2:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-2
    container_name: kafka-2
    ports:
      - "9093:9092"
      - "29093:29092"
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-2:29092,EXTERNAL://localhost:9093"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka-2-data:/var/lib/kafka/data
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:29092"]
      interval: 10s
      timeout: 10s
      retries: 5

  # --- Kafka Broker 3 ---
  kafka-3:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-3
    container_name: kafka-3
    ports:
      - "9094:9092"
      - "29094:29092"
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://kafka-3:29092,EXTERNAL://localhost:9094"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka-3-data:/var/lib/kafka/data
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:29092"]
      interval: 10s
      timeout: 10s
      retries: 5

  # --- Schema Registry ---
  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry
    container_name: schema-registry
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka-1:29092,kafka-2:29092,kafka-3:29092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "BACKWARD"
      SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL: "BACKWARD"
    depends_on:
      kafka-1:
        condition: service_healthy
      kafka-2:
        condition: service_healthy
      kafka-3:
        condition: service_healthy
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

  # --- Kafka UI ---
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    hostname: kafka-ui
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: "local-cluster"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka-1:29092,kafka-2:29092,kafka-3:29092"
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: "http://schema-registry:8081"
      KAFKA_CLUSTERS_0_METRICS_PORT: 9997
      DYNAMIC_CONFIG_ENABLED: "true"
    depends_on:
      schema-registry:
        condition: service_healthy
    networks:
      - kafka-network

  # --- Topic Initialization ---
  kafka-init:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka-init
    depends_on:
      kafka-1:
        condition: service_healthy
      kafka-2:
        condition: service_healthy
      kafka-3:
        condition: service_healthy
    entrypoint: ["/bin/bash", "-c"]
    command: |
      "
      echo 'Creating Kafka topics...'

      kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
        --topic raw.trades \
        --partitions 12 \
        --replication-factor 3 \
        --config retention.ms=604800000 \
        --config min.insync.replicas=2 \
        --config cleanup.policy=delete \
        --config segment.bytes=1073741824

      kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
        --topic raw.orderbook \
        --partitions 12 \
        --replication-factor 3 \
        --config retention.ms=604800000 \
        --config min.insync.replicas=2 \
        --config cleanup.policy=delete

      kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
        --topic raw.marketdata \
        --partitions 12 \
        --replication-factor 3 \
        --config retention.ms=604800000 \
        --config min.insync.replicas=2 \
        --config cleanup.policy=delete

      kafka-topics --bootstrap-server kafka-1:29092 --create --if-not-exists \
        --topic dlq.raw.failed \
        --partitions 3 \
        --replication-factor 3 \
        --config retention.ms=2592000000 \
        --config min.insync.replicas=2 \
        --config cleanup.policy=delete

      echo 'Topics created successfully.'
      kafka-topics --bootstrap-server kafka-1:29092 --list
      "
    networks:
      - kafka-network

volumes:
  kafka-1-data:
  kafka-2-data:
  kafka-3-data:

networks:
  kafka-network:
    driver: bridge
    name: kafka-network
```

### 4.3 Topic Design

| Topic              | Partitions | Replication | Retention | Min ISR | Cleanup Policy | Purpose                          |
|--------------------|------------|-------------|-----------|---------|----------------|----------------------------------|
| `raw.trades`       | 12         | 3           | 7 days    | 2       | delete         | All incoming trade events        |
| `raw.orderbook`    | 12         | 3           | 7 days    | 2       | delete         | Order book snapshots             |
| `raw.marketdata`   | 12         | 3           | 7 days    | 2       | delete         | OHLCV market data candles        |
| `dlq.raw.failed`   | 3          | 3           | 30 days   | 2       | delete         | Dead letter queue for failures   |

**Partition Key**: `symbol` (string) for all raw topics.

**Partition count rationale**: 12 partitions with ~50 symbols means roughly 4 symbols per partition. This provides a good balance:
- Enough parallelism for downstream Flink consumers
- Few enough to avoid excessive overhead
- Divisible by common consumer group sizes (1, 2, 3, 4, 6, 12)

### 4.4 Schema Registry Configuration

**Compatibility Mode**: BACKWARD (default)

Subject naming convention follows **TopicNameStrategy**:

| Topic              | Value Subject              | Key Subject              |
|--------------------|---------------------------|--------------------------|
| `raw.trades`       | `raw.trades-value`        | `raw.trades-key`         |
| `raw.orderbook`    | `raw.orderbook-value`     | `raw.orderbook-key`      |
| `raw.marketdata`   | `raw.marketdata-value`    | `raw.marketdata-key`     |

Schema evolution strategy:
- **BACKWARD** compatibility: new schema can read data written with the old schema.
- Adding a new optional field (with default): compatible.
- Removing an optional field: compatible.
- Adding a required field without default: **NOT** compatible (breaking change, requires new topic version).
- Changing field type: **NOT** compatible.

---

## 5. Avro Schema Definitions

### 5.1 Trade Schema

```json
// libs/common/schemas/avro/trade.avsc
{
  "type": "record",
  "name": "Trade",
  "namespace": "com.tradeplatform.events",
  "doc": "Represents a stock market trade event (order placement, fill, cancellation).",
  "fields": [
    {
      "name": "trade_id",
      "type": "string",
      "doc": "Unique trade identifier (UUID v4)"
    },
    {
      "name": "symbol",
      "type": "string",
      "doc": "Stock ticker symbol (e.g., AAPL, GOOGL)"
    },
    {
      "name": "trade_type",
      "type": {
        "type": "enum",
        "name": "TradeType",
        "symbols": ["BUY", "SELL"]
      },
      "doc": "Direction of the trade"
    },
    {
      "name": "order_type",
      "type": {
        "type": "enum",
        "name": "OrderType",
        "symbols": ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
      },
      "doc": "Type of order"
    },
    {
      "name": "price",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Trade price with 4 decimal places"
    },
    {
      "name": "quantity",
      "type": "int",
      "doc": "Number of shares"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-micros"
      },
      "doc": "Event timestamp in UTC microseconds since epoch"
    },
    {
      "name": "exchange",
      "type": {
        "type": "enum",
        "name": "Exchange",
        "symbols": ["NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"]
      },
      "doc": "Exchange where the trade was executed"
    },
    {
      "name": "trader_id",
      "type": "string",
      "doc": "Identifier of the trader who placed the order"
    },
    {
      "name": "account_id",
      "type": "string",
      "doc": "Trading account identifier"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "TradeStatus",
        "symbols": ["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"]
      },
      "doc": "Current status of the trade/order"
    },
    {
      "name": "correlation_id",
      "type": ["null", "string"],
      "default": null,
      "doc": "Links related events in an order lifecycle (e.g., order to fill)"
    },
    {
      "name": "schema_version",
      "type": "int",
      "default": 1,
      "doc": "Schema version for forward compatibility"
    },
    {
      "name": "source",
      "type": "string",
      "default": "trade-generator",
      "doc": "Source system identifier"
    }
  ]
}
```

### 5.2 OrderBook Schema

```json
// libs/common/schemas/avro/orderbook.avsc
{
  "type": "record",
  "name": "OrderBook",
  "namespace": "com.tradeplatform.events",
  "doc": "Order book snapshot for a given symbol.",
  "fields": [
    {
      "name": "symbol",
      "type": "string",
      "doc": "Stock ticker symbol"
    },
    {
      "name": "bid_price",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Best bid price"
    },
    {
      "name": "ask_price",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Best ask price"
    },
    {
      "name": "bid_size",
      "type": "int",
      "doc": "Total shares at best bid"
    },
    {
      "name": "ask_size",
      "type": "int",
      "doc": "Total shares at best ask"
    },
    {
      "name": "spread",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Bid-ask spread (ask_price - bid_price)"
    },
    {
      "name": "mid_price",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Mid price ((bid + ask) / 2)"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-micros"
      },
      "doc": "Snapshot timestamp in UTC microseconds since epoch"
    },
    {
      "name": "exchange",
      "type": "string",
      "doc": "Exchange identifier"
    },
    {
      "name": "schema_version",
      "type": "int",
      "default": 1,
      "doc": "Schema version"
    },
    {
      "name": "source",
      "type": "string",
      "default": "trade-generator",
      "doc": "Source system identifier"
    }
  ]
}
```

### 5.3 MarketData Schema

```json
// libs/common/schemas/avro/marketdata.avsc
{
  "type": "record",
  "name": "MarketData",
  "namespace": "com.tradeplatform.events",
  "doc": "OHLCV market data candle for a given symbol and interval.",
  "fields": [
    {
      "name": "symbol",
      "type": "string",
      "doc": "Stock ticker symbol"
    },
    {
      "name": "open",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Opening price of the candle"
    },
    {
      "name": "high",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Highest price during the candle"
    },
    {
      "name": "low",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Lowest price during the candle"
    },
    {
      "name": "close",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Closing price of the candle"
    },
    {
      "name": "volume",
      "type": "long",
      "doc": "Total volume during the candle"
    },
    {
      "name": "vwap",
      "type": {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 4
      },
      "doc": "Volume Weighted Average Price"
    },
    {
      "name": "timestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-micros"
      },
      "doc": "Candle start timestamp in UTC microseconds since epoch"
    },
    {
      "name": "interval",
      "type": "string",
      "default": "1m",
      "doc": "Candle interval (1m, 5m, 15m, 1h, 1d)"
    },
    {
      "name": "exchange",
      "type": "string",
      "doc": "Exchange identifier"
    },
    {
      "name": "schema_version",
      "type": "int",
      "default": 1,
      "doc": "Schema version"
    },
    {
      "name": "source",
      "type": "string",
      "default": "trade-generator",
      "doc": "Source system identifier"
    }
  ]
}
```

### 5.4 Schema Evolution Rules

| Change                                | Compatibility | Action Required              |
|---------------------------------------|---------------|------------------------------|
| Add optional field with default        | BACKWARD      | Register new schema version  |
| Remove optional field                  | BACKWARD      | Register new schema version  |
| Add required field without default     | BREAKING      | Requires new topic or migration |
| Change field type                      | BREAKING      | Requires new topic or migration |
| Rename field                           | BREAKING      | Requires new topic or migration |
| Add new enum value                     | BACKWARD      | Register new schema version  |
| Remove enum value                      | BREAKING      | Requires careful migration   |

### 5.5 Schema Registration Script

```bash
#!/bin/bash
# infrastructure/scripts/register-schemas.sh

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="libs/common/schemas/avro"

register_schema() {
  local subject="$1"
  local schema_file="$2"

  echo "Registering schema: ${subject} from ${schema_file}"

  # Escape the JSON for embedding in the request body
  schema_json=$(cat "${schema_file}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')

  curl -s -X POST "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\": \"AVRO\", \"schema\": ${schema_json}}" | python3 -m json.tool

  echo ""
}

# Register value schemas
register_schema "raw.trades-value" "${SCHEMA_DIR}/trade.avsc"
register_schema "raw.orderbook-value" "${SCHEMA_DIR}/orderbook.avsc"
register_schema "raw.marketdata-value" "${SCHEMA_DIR}/marketdata.avsc"

# Set compatibility mode
for subject in "raw.trades-value" "raw.orderbook-value" "raw.marketdata-value"; do
  echo "Setting BACKWARD compatibility for ${subject}"
  curl -s -X PUT "${SCHEMA_REGISTRY_URL}/config/${subject}" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}'
  echo ""
done

echo "All schemas registered."
```

---

## 6. Data Contracts

### 6.1 Producer-Consumer Contract

The contract between the Trade Data Generator (producer) and the Kafka Bridge (consumer) is defined by:

**Transport**: WebSocket (`ws://host:port`)

**Message Envelope**:

```
{
  "schema_version": <int>,       // Required. Current: 1
  "event_type": <string>,        // Required. One of: "trade", "orderbook", "marketdata"
  "timestamp": <ISO 8601 UTC>,   // Required. Server-side envelope timestamp
  "payload": <object>            // Required. Event-specific fields per schema
}
```

**Contract Rules**:
1. The producer MUST include all required fields in the envelope.
2. The producer MUST set `schema_version` to a valid integer corresponding to a registered schema.
3. The producer MUST set `event_type` to one of the defined types.
4. Payload fields MUST match the Avro schema for the given `event_type` and `schema_version`.
5. Timestamps MUST be in UTC.
6. Decimal values (prices) MUST be serialized as strings in JSON (e.g., `"178.52"`) to avoid floating-point precision issues.
7. UUIDs MUST be serialized as lowercase hex with dashes (e.g., `"550e8400-e29b-41d4-a716-446655440000"`).

### 6.2 Schema Validation at Ingestion

The Kafka Bridge validates every incoming WebSocket message before producing to Kafka:

```python
# validation/message_validator.py
import jsonschema
from typing import Tuple

ENVELOPE_SCHEMA = {
    "type": "object",
    "required": ["schema_version", "event_type", "timestamp", "payload"],
    "properties": {
        "schema_version": {"type": "integer", "minimum": 1},
        "event_type": {"type": "string", "enum": ["trade", "orderbook", "marketdata"]},
        "timestamp": {"type": "string", "format": "date-time"},
        "payload": {"type": "object"},
    },
    "additionalProperties": False,
}

TRADE_PAYLOAD_SCHEMA = {
    "type": "object",
    "required": [
        "trade_id", "symbol", "trade_type", "order_type",
        "price", "quantity", "timestamp", "exchange",
        "trader_id", "account_id", "status"
    ],
    "properties": {
        "trade_id": {"type": "string", "format": "uuid"},
        "symbol": {"type": "string", "minLength": 1, "maxLength": 10},
        "trade_type": {"type": "string", "enum": ["BUY", "SELL"]},
        "order_type": {"type": "string", "enum": ["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]},
        "price": {"type": "string", "pattern": "^\\d+\\.\\d{2,4}$"},
        "quantity": {"type": "integer", "minimum": 1},
        "timestamp": {"type": "string"},
        "exchange": {"type": "string", "enum": ["NYSE", "NASDAQ", "AMEX", "ARCA", "BATS"]},
        "trader_id": {"type": "string", "pattern": "^TRD-\\d{5}$"},
        "account_id": {"type": "string", "pattern": "^ACC-\\d{4}$"},
        "status": {
            "type": "string",
            "enum": ["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"]
        },
        "correlation_id": {"type": ["string", "null"]},
    },
}

# Similar schemas for orderbook and marketdata payloads...

PAYLOAD_SCHEMAS = {
    "trade": TRADE_PAYLOAD_SCHEMA,
    # "orderbook": ORDERBOOK_PAYLOAD_SCHEMA,
    # "marketdata": MARKETDATA_PAYLOAD_SCHEMA,
}

class MessageValidator:
    """Validates incoming WebSocket messages against JSON schemas."""

    def validate(self, message: dict) -> Tuple[bool, str | None]:
        """
        Returns (is_valid, error_message).
        """
        # 1. Validate envelope
        try:
            jsonschema.validate(message, ENVELOPE_SCHEMA)
        except jsonschema.ValidationError as e:
            return False, f"Envelope validation failed: {e.message}"

        # 2. Validate payload
        event_type = message["event_type"]
        payload_schema = PAYLOAD_SCHEMAS.get(event_type)
        if payload_schema is None:
            return False, f"No payload schema for event_type: {event_type}"

        try:
            jsonschema.validate(message["payload"], payload_schema)
        except jsonschema.ValidationError as e:
            return False, f"Payload validation failed: {e.message}"

        return True, None
```

### 6.3 SLA Definitions

| Metric                     | SLA Target          | Measurement Point                        |
|----------------------------|---------------------|------------------------------------------|
| **End-to-end latency**     | < 500ms (p99)       | Generator timestamp to Kafka produce ack |
| **Throughput**              | >= 1,000 msgs/sec   | Kafka produce rate (sustained)           |
| **Availability**            | 99.9%               | Kafka cluster uptime                     |
| **Data loss**               | 0 messages          | `acks=all` + `min.insync.replicas=2`     |
| **Schema validation**       | 100% of messages    | Bridge validates before producing        |
| **WebSocket reconnection**  | < 120s              | Maximum time to re-establish connection  |
| **DLQ processing**          | Review within 24hrs | Manual triage of DLQ messages            |

---

## 7. Implementation Steps

The following is the ordered list of files and directories to create. Each step builds on the previous.

### Phase 1: Shared Libraries (libs/common)

| Step | Path                                            | Description                                    |
|------|-------------------------------------------------|------------------------------------------------|
| 1.1  | `libs/common/pyproject.toml`                    | Shared library package definition              |
| 1.2  | `libs/common/src/common/__init__.py`            | Package init                                   |
| 1.3  | `libs/common/src/common/models/__init__.py`     | Models package init                            |
| 1.4  | `libs/common/src/common/models/enums.py`        | TradeType, OrderType, TradeStatus, Exchange     |
| 1.5  | `libs/common/src/common/models/trade.py`        | Trade dataclass                                |
| 1.6  | `libs/common/src/common/models/orderbook.py`    | OrderBook dataclass                            |
| 1.7  | `libs/common/src/common/models/market_data.py`  | MarketData dataclass                           |
| 1.8  | `libs/common/schemas/avro/trade.avsc`           | Trade Avro schema                              |
| 1.9  | `libs/common/schemas/avro/orderbook.avsc`       | OrderBook Avro schema                          |
| 1.10 | `libs/common/schemas/avro/marketdata.avsc`      | MarketData Avro schema                         |
| 1.11 | `libs/common/schemas/json/envelope.json`        | JSON schema for WebSocket envelope             |
| 1.12 | `libs/common/schemas/json/trade_payload.json`   | JSON schema for trade payload validation       |
| 1.13 | `libs/common/schemas/json/orderbook_payload.json`| JSON schema for orderbook payload validation  |
| 1.14 | `libs/common/schemas/json/marketdata_payload.json`| JSON schema for marketdata payload validation|

### Phase 2: Trade Data Generator (services/trade-generator)

| Step | Path                                                              | Description                           |
|------|-------------------------------------------------------------------|---------------------------------------|
| 2.1  | `services/trade-generator/pyproject.toml`                         | Package definition with dependencies  |
| 2.2  | `services/trade-generator/src/trade_generator/__init__.py`        | Package init                          |
| 2.3  | `services/trade-generator/src/trade_generator/__main__.py`        | Entry point                           |
| 2.4  | `services/trade-generator/src/trade_generator/cli.py`             | CLI argument parsing                  |
| 2.5  | `services/trade-generator/src/trade_generator/config/settings.py` | Pydantic settings                     |
| 2.6  | `services/trade-generator/src/trade_generator/config/symbols.json`| Symbol reference data (50 stocks)     |
| 2.7  | `services/trade-generator/src/trade_generator/engine/price_simulator.py` | GBM price simulation           |
| 2.8  | `services/trade-generator/src/trade_generator/engine/symbol_registry.py` | Symbol metadata loader         |
| 2.9  | `services/trade-generator/src/trade_generator/engine/trade_engine.py`    | Trade event generation         |
| 2.10 | `services/trade-generator/src/trade_generator/engine/orderbook_engine.py`| Order book snapshot generation |
| 2.11 | `services/trade-generator/src/trade_generator/engine/market_data_engine.py`| Market data candle generation|
| 2.12 | `services/trade-generator/src/trade_generator/serialization/json_serializer.py`| JSON serialization     |
| 2.13 | `services/trade-generator/src/trade_generator/server.py`          | WebSocket server                      |
| 2.14 | `services/trade-generator/tests/conftest.py`                      | Test fixtures                         |
| 2.15 | `services/trade-generator/tests/test_price_simulator.py`          | Price simulator unit tests            |
| 2.16 | `services/trade-generator/tests/test_trade_engine.py`             | Trade engine unit tests               |
| 2.17 | `services/trade-generator/tests/test_orderbook_engine.py`         | Order book engine tests               |
| 2.18 | `services/trade-generator/tests/test_market_data_engine.py`       | Market data engine tests              |
| 2.19 | `services/trade-generator/tests/test_server.py`                   | WebSocket server integration tests    |
| 2.20 | `services/trade-generator/tests/test_cli.py`                      | CLI parsing tests                     |
| 2.21 | `services/trade-generator/Dockerfile`                             | Container image for deployment        |

### Phase 3: Kafka Infrastructure (infrastructure/)

| Step | Path                                                                    | Description                          |
|------|-------------------------------------------------------------------------|--------------------------------------|
| 3.1  | `infrastructure/docker-compose/docker-compose.kafka.yml`                | 3-broker KRaft Kafka + Schema Reg    |
| 3.2  | `infrastructure/scripts/register-schemas.sh`                            | Schema registration script           |
| 3.3  | `infrastructure/scripts/create-topics.sh`                               | Topic creation script (standalone)   |
| 3.4  | `infrastructure/scripts/verify-cluster.sh`                              | Cluster health verification          |

### Phase 4: WebSocket-Kafka Bridge (services/kafka-bridge)

| Step | Path                                                                    | Description                           |
|------|-------------------------------------------------------------------------|---------------------------------------|
| 4.1  | `services/kafka-bridge/pyproject.toml`                                  | Package definition with dependencies  |
| 4.2  | `services/kafka-bridge/src/kafka_bridge/__init__.py`                    | Package init                          |
| 4.3  | `services/kafka-bridge/src/kafka_bridge/__main__.py`                    | Entry point                           |
| 4.4  | `services/kafka-bridge/src/kafka_bridge/cli.py`                         | CLI argument parsing                  |
| 4.5  | `services/kafka-bridge/src/kafka_bridge/config/settings.py`             | Pydantic settings                     |
| 4.6  | `services/kafka-bridge/src/kafka_bridge/websocket_client.py`            | WebSocket consumer with reconnect     |
| 4.7  | `services/kafka-bridge/src/kafka_bridge/kafka_producer.py`              | Kafka producer with Avro serialization|
| 4.8  | `services/kafka-bridge/src/kafka_bridge/message_router.py`              | Event type to topic routing           |
| 4.9  | `services/kafka-bridge/src/kafka_bridge/bridge.py`                      | Main bridge orchestrator              |
| 4.10 | `services/kafka-bridge/src/kafka_bridge/serialization/avro_serializer.py`| Avro serializer with Schema Registry|
| 4.11 | `services/kafka-bridge/src/kafka_bridge/validation/message_validator.py` | JSON schema validation              |
| 4.12 | `services/kafka-bridge/src/kafka_bridge/error_handling/dlq.py`          | Dead letter queue handler            |
| 4.13 | `services/kafka-bridge/src/kafka_bridge/error_handling/retry.py`        | Retry with exponential backoff       |
| 4.14 | `services/kafka-bridge/src/kafka_bridge/metrics/prometheus.py`          | Prometheus metric definitions        |
| 4.15 | `services/kafka-bridge/tests/conftest.py`                               | Test fixtures                        |
| 4.16 | `services/kafka-bridge/tests/test_bridge.py`                            | End-to-end bridge tests              |
| 4.17 | `services/kafka-bridge/tests/test_websocket_client.py`                  | WebSocket client tests               |
| 4.18 | `services/kafka-bridge/tests/test_kafka_producer.py`                    | Kafka producer tests                 |
| 4.19 | `services/kafka-bridge/tests/test_message_router.py`                    | Message routing tests                |
| 4.20 | `services/kafka-bridge/tests/test_message_validator.py`                 | Validation tests                     |
| 4.21 | `services/kafka-bridge/Dockerfile`                                      | Container image for deployment       |

### Phase 5: Integration and Verification

| Step | Path                                               | Description                                  |
|------|----------------------------------------------------|----------------------------------------------|
| 5.1  | `tests/integration/test_e2e_ingestion.py`          | Full pipeline: generator -> WS -> Kafka      |
| 5.2  | `tests/integration/test_schema_registry.py`        | Schema registration and evolution tests      |
| 5.3  | `tests/load/locustfile.py`                         | Load testing with Locust                     |
| 5.4  | `Makefile` (append targets)                        | `make kafka-up`, `make generator`, `make bridge` |

---

## 8. Testing Strategy

### 8.1 Unit Tests

**Trade Generator Tests**:

| Test Area            | Test Cases                                                                 |
|----------------------|----------------------------------------------------------------------------|
| `PriceSimulator`     | Prices stay positive; volatility affects spread; seed reproducibility       |
| `TradeEngine`        | Correct field population; weighted symbol selection; order lifecycle states |
| `OrderBookEngine`    | Spread is always positive; bid < ask; sizes are realistic                  |
| `MarketDataEngine`   | OHLC invariant (low <= open,close <= high); volume > 0; VWAP within range  |
| `SymbolRegistry`     | All 50 symbols loaded; weights sum to ~1.0; valid exchanges               |
| `JsonSerializer`     | Correct envelope format; decimal precision; UUID format; timestamp format   |
| `CLI`                | Argument defaults; argument validation; invalid argument handling           |

**Kafka Bridge Tests**:

| Test Area              | Test Cases                                                                |
|------------------------|---------------------------------------------------------------------------|
| `MessageValidator`     | Valid messages pass; missing fields rejected; invalid types rejected       |
| `MessageRouter`        | Correct topic mapping; unknown event type raises error                    |
| `DLQProducer`          | Failed messages include original payload + error metadata                 |
| `RetryHandler`         | Retries up to max; sends to DLQ after exhaustion                         |
| `PrometheusMetrics`    | Counters increment; histograms observe; labels are correct               |

### 8.2 Integration Tests

**WebSocket Integration**:
```python
# tests/integration/test_e2e_ingestion.py
import pytest
import asyncio
from websockets.asyncio.client import connect

@pytest.mark.asyncio
async def test_trade_generator_streams_trades():
    """Start the generator, connect as client, verify trade format."""
    async with connect("ws://localhost:8765") as ws:
        message = await asyncio.wait_for(ws.recv(), timeout=5.0)
        data = json.loads(message)
        assert "schema_version" in data
        assert "event_type" in data
        assert "payload" in data
        assert data["event_type"] in ("trade", "orderbook", "marketdata")
```

**Kafka Integration** (using testcontainers):
```python
@pytest.mark.integration
async def test_bridge_produces_to_kafka():
    """
    1. Start Kafka via testcontainers
    2. Start trade generator
    3. Start bridge
    4. Consume from raw.trades and verify messages
    """
    # Uses testcontainers-python for Kafka + Schema Registry
    # Verifies: message arrives in correct topic, Avro deserialization works,
    # partition key matches symbol
```

### 8.3 Load Testing

Use **Locust** or a custom async script:

```python
# tests/load/locustfile.py
"""
Load testing scenarios:
1. Steady state: 100 trades/sec for 10 minutes
2. Burst: Ramp from 10 to 5000 trades/sec over 30 seconds
3. Soak: 50 trades/sec for 1 hour (verify no memory leaks)
"""

# Metrics to capture:
# - End-to-end latency (generator timestamp -> Kafka produce ack)
# - Kafka producer queue depth
# - WebSocket message backpressure
# - CPU and memory of each service
# - Kafka broker disk I/O
```

Load testing targets:

| Scenario       | Rate        | Duration | Success Criteria                                  |
|----------------|-------------|----------|---------------------------------------------------|
| Steady state   | 100 msg/s   | 10 min   | p99 latency < 500ms, 0 messages lost              |
| Burst          | 5,000 msg/s | 30 sec   | No OOM, queue depth recovers within 60s            |
| Soak           | 50 msg/s    | 1 hour   | No memory growth > 10%, stable latency             |
| Recovery       | N/A         | N/A      | After Kafka broker restart, bridge reconnects < 60s|

---

## 8. Configuration

### 8.1 Trade Generator Configuration

```python
# config/settings.py
from pydantic_settings import BaseSettings

class TradeGeneratorSettings(BaseSettings):
    """Configuration loaded from environment variables or .env file."""

    # Server
    ws_host: str = "0.0.0.0"
    ws_port: int = 8765

    # Generation
    rate: float = 10.0                # Trades per second
    num_symbols: int = 50             # Number of active symbols
    mode: str = "normal"              # normal / burst / historical
    seed: int | None = None           # Random seed

    # Burst mode
    burst_multiplier: float = 20.0
    burst_interval_seconds: int = 60
    burst_duration_seconds: int = 10

    # Market simulation
    simulate_market_hours: bool = True
    market_timezone: str = "US/Eastern"
    start_date: str | None = None     # For historical mode (YYYY-MM-DD)

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"          # json or text

    model_config = {"env_prefix": "TRADE_GEN_"}
```

### 8.2 Kafka Bridge Configuration

```python
# config/settings.py
from pydantic_settings import BaseSettings

class KafkaBridgeSettings(BaseSettings):
    """Configuration loaded from environment variables or .env file."""

    # WebSocket source
    ws_uri: str = "ws://localhost:8765"
    ws_max_retries: int = 10
    ws_base_backoff: float = 1.0
    ws_max_backoff: float = 60.0

    # Kafka producer
    kafka_bootstrap_servers: str = "localhost:9092,localhost:9093,localhost:9094"
    kafka_client_id: str = "trade-bridge-producer"
    kafka_acks: str = "all"
    kafka_batch_size: int = 65536
    kafka_linger_ms: int = 10
    kafka_compression_type: str = "snappy"
    kafka_enable_idempotence: bool = True

    # Schema Registry
    schema_registry_url: str = "http://localhost:8081"

    # Metrics
    metrics_enabled: bool = True
    metrics_port: int = 9090

    # DLQ
    dlq_topic: str = "dlq.raw.failed"
    dlq_max_retries: int = 3

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    model_config = {"env_prefix": "KAFKA_BRIDGE_"}
```

### 8.3 Environment-Specific Overrides

Configuration is loaded in this priority order (highest wins):

1. CLI arguments (for the trade generator)
2. Environment variables (prefixed with `TRADE_GEN_` or `KAFKA_BRIDGE_`)
3. `.env` file (loaded by pydantic-settings)
4. Default values in the Settings class

Example `.env` files:

```bash
# environments/dev.env
TRADE_GEN_RATE=10
TRADE_GEN_NUM_SYMBOLS=10
TRADE_GEN_LOG_LEVEL=DEBUG
KAFKA_BRIDGE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_BRIDGE_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_BRIDGE_LOG_LEVEL=DEBUG
```

```bash
# environments/load-test.env
TRADE_GEN_RATE=1000
TRADE_GEN_NUM_SYMBOLS=50
TRADE_GEN_MODE=burst
TRADE_GEN_BURST_MULTIPLIER=50
TRADE_GEN_LOG_LEVEL=WARNING
KAFKA_BRIDGE_KAFKA_BATCH_SIZE=131072
KAFKA_BRIDGE_KAFKA_LINGER_MS=50
KAFKA_BRIDGE_LOG_LEVEL=WARNING
```

### 8.4 Tunable Parameters Summary

| Parameter                         | Default  | Range / Notes                          | Impact                                |
|-----------------------------------|----------|----------------------------------------|---------------------------------------|
| `rate` (trades/sec)               | 10       | 1 - 10,000                             | Higher = more CPU, network, Kafka load|
| `num_symbols`                     | 50       | 1 - 50                                 | Fewer symbols = less partition spread  |
| `kafka_batch_size`                | 64 KB    | 16 KB - 1 MB                           | Larger = higher throughput, latency   |
| `kafka_linger_ms`                 | 10       | 0 - 100                                | Higher = more batching, higher latency|
| `kafka_compression_type`          | snappy   | none, gzip, snappy, lz4, zstd          | snappy: best speed; zstd: best ratio  |
| `kafka_acks`                      | all      | 0, 1, all                              | all: highest durability               |
| `ws_base_backoff`                 | 1.0s     | 0.1 - 5.0                              | Lower = faster reconnect, more noise  |
| `burst_multiplier`                | 20x      | 2x - 100x                              | Higher = more extreme spikes          |
| `kafka partitions` (raw.trades)   | 12       | 6 - 50                                 | More = better parallelism             |

---

## 10. Appendix

### 10.1 Monorepo Directory Structure (Ingestion-Related)

```
DE-project/
|-- plan/
|   |-- high-level-requirement.md
|   |-- 02-data-sources-and-ingestion.md    # This document
|-- libs/
|   |-- common/
|       |-- pyproject.toml
|       |-- src/common/
|       |   |-- models/
|       |   |   |-- enums.py
|       |   |   |-- trade.py
|       |   |   |-- orderbook.py
|       |   |   |-- market_data.py
|       |-- schemas/
|           |-- avro/
|           |   |-- trade.avsc
|           |   |-- orderbook.avsc
|           |   |-- marketdata.avsc
|           |-- json/
|               |-- envelope.json
|               |-- trade_payload.json
|               |-- orderbook_payload.json
|               |-- marketdata_payload.json
|-- services/
|   |-- trade-generator/
|   |   |-- pyproject.toml
|   |   |-- Dockerfile
|   |   |-- src/trade_generator/
|   |   |-- tests/
|   |-- kafka-bridge/
|       |-- pyproject.toml
|       |-- Dockerfile
|       |-- src/kafka_bridge/
|       |-- tests/
|-- infrastructure/
|   |-- docker-compose/
|   |   |-- docker-compose.kafka.yml
|   |-- scripts/
|       |-- register-schemas.sh
|       |-- create-topics.sh
|       |-- verify-cluster.sh
|-- tests/
|   |-- integration/
|   |   |-- test_e2e_ingestion.py
|   |   |-- test_schema_registry.py
|   |-- load/
|       |-- locustfile.py
|-- environments/
|   |-- dev.env
|   |-- load-test.env
|-- Makefile
```

### 10.2 Makefile Targets

```makefile
# Data Sources & Ingestion targets

.PHONY: kafka-up kafka-down generator bridge schemas

kafka-up:
	docker compose -f infrastructure/docker-compose/docker-compose.kafka.yml up -d

kafka-down:
	docker compose -f infrastructure/docker-compose/docker-compose.kafka.yml down -v

kafka-logs:
	docker compose -f infrastructure/docker-compose/docker-compose.kafka.yml logs -f

schemas:
	./infrastructure/scripts/register-schemas.sh

generator:
	cd services/trade-generator && python -m trade_generator --rate 10 --symbols 50 --mode normal

bridge:
	cd services/kafka-bridge && python -m kafka_bridge

test-generator:
	cd services/trade-generator && pytest tests/ -v --cov=trade_generator

test-bridge:
	cd services/kafka-bridge && pytest tests/ -v --cov=kafka_bridge

test-integration:
	pytest tests/integration/ -v -m integration

test-load:
	cd tests/load && locust -f locustfile.py
```

### 10.3 Quick Start Sequence

```bash
# 1. Start Kafka infrastructure
make kafka-up

# 2. Wait for cluster health (takes ~30 seconds)
./infrastructure/scripts/verify-cluster.sh

# 3. Register Avro schemas
make schemas

# 4. Start the trade generator (terminal 1)
make generator

# 5. Start the Kafka bridge (terminal 2)
make bridge

# 6. Open Kafka UI to monitor
open http://localhost:8080

# 7. Verify messages are flowing
docker exec kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic raw.trades \
  --from-beginning \
  --max-messages 5
```
