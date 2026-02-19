# 02 - Data Sources & Ingestion: Implementation Plan

> **Component**: Phase 1 & 2 -- Data Sources & Ingestion
> **Parent Document**: [High-Level Requirement](./high-level-requirement.md)
> **Last Updated**: 2026-02-18
> **Status**: Draft
> **Data Source**: [DE-Stock Exchange Simulator](https://github.com/dileepbapat/mock-stock)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [DE-Stock Exchange Simulator](#2-de-stock-exchange-simulator)
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
|  DE-Stock Exchange  | =========================> |  WebSocket-Kafka    |
|  Simulator          |   JSON messages            |  Bridge             |
|  (Rust Binary)      |   (9 event types)          |  (Kafka Producer)   |
|                     |                            |                     |
+---------------------+                            +---------------------+
        |                                                    |
        | Produces:                                          | Routes to 8 topics:
        |  - OrderPlaced                                     |  - raw.orders
        |  - TradeExecuted                                   |  - raw.trades
        |  - QuoteUpdate                                     |  - raw.quotes
        |  - OrderBookSnapshot                               |  - raw.orderbook-snapshots
        |  - TradingHalt / TradingResume                     |  - raw.trading-halts
        |  - AgentAction                                     |  - raw.agent-actions
        |  - MarketStats                                     |  - raw.market-stats
        |  - OrderCancelled                                  |  - dlq.raw.failed
        |                                                    |
        v                                                    v
  TOML Config                                      +---------------------+
  (symbols, agents,                                |  Kafka Cluster      |
   tick rate,                                      |  (3 brokers, KRaft) |
   circuit breakers)                               |                     |
                                                   |  + Schema Registry  |
                                                   |  + Kafka UI         |
                                                   +---------------------+
```

**Data Flow Summary**:

1. The **DE-Stock Exchange Simulator** runs a full limit order book with price-time priority matching, 6 agent types, circuit breakers, and configurable data quality issues. It streams 9 event types over a WebSocket server in JSON format.
2. The **WebSocket-Kafka Bridge** connects as a WebSocket client, deserializes JSON messages, validates them against Pydantic models, serializes to Avro, and routes them to the appropriate Kafka topic (8 topics total).
3. **Kafka** stores the raw events durably across a 3-broker KRaft cluster with schema enforcement via Confluent Schema Registry.

**Key Differences from a Simple Price Generator**:

| Aspect | Simple Generator | DE-Stock |
|--------|-----------------|----------|
| Market model | GBM random walk | Full limit order book with matching engine |
| Agents | None (random) | 6 types: Retail, Institutional, Market Maker, HFT, Noise, Informed |
| Event types | 3 (Trade, OrderBook, MarketData) | 9 (see Section 2.2) |
| Price discovery | Simulated externally | Emergent from order book supply/demand |
| Circuit breakers | None | 3 levels (7%/13%/20%) |
| Data quality | Perfect | Configurable duplicates, out-of-order, missing data |
| Implementation | Python | Rust (high performance, low latency) |

---

## 2. DE-Stock Exchange Simulator

### 2.1 Application Design

**Source**: [github.com/dileepbapat/mock-stock](https://github.com/dileepbapat/mock-stock)
**Language**: Rust (2021 edition)
**Binary Name**: `de-stock` (from Cargo.toml)
**Transport**: WebSocket (tokio-tungstenite)
**Configuration**: TOML file passed as CLI argument
**Key Dependencies**: tokio, tokio-tungstenite, serde/serde_json, rand/rand_distr, chrono, ordered-float, uuid

The simulator is a self-contained Rust binary that:
1. Reads a TOML configuration file specifying symbols, agents, and simulation parameters
2. Initializes a limit order book per symbol
3. Spawns agents that submit orders based on their strategy
4. Matches orders using price-time priority
5. Streams all events (orders, trades, quotes, snapshots, halts, stats) over a WebSocket server

```
services/de-stock/
|-- Dockerfile                    # Multi-stage Rust build
|-- config/
|   |-- default.toml              # Production config (20+ symbols)
|   |-- test.toml                 # Minimal config for testing (2 symbols)
```

### 2.2 Event Types (9 Types)

DE-Stock produces 9 distinct event types, each with a specific structure:

#### 2.2.1 OrderPlaced

Emitted when an agent submits a new order to the order book.

```json
{
  "event_type": "OrderPlaced",
  "timestamp": "2026-02-18T14:30:00.123456Z",
  "data": {
    "order_id": "550e8400-e29b-41d4-a716-446655440000",
    "symbol": "AAPL",
    "side": "Buy",
    "order_type": "Limit",
    "price": 185.50,
    "quantity": 100,
    "agent_id": "retail-0042",
    "agent_type": "Retail"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | UUID v4 | Unique order identifier |
| `symbol` | string | Stock ticker (e.g., AAPL) |
| `side` | enum | `Buy` or `Sell` |
| `order_type` | enum | `Market` or `Limit` |
| `price` | float | Order price (0.0 for market orders) |
| `quantity` | int | Number of shares |
| `agent_id` | string | Identifier of the submitting agent |
| `agent_type` | enum | One of 6 agent types |

#### 2.2.2 TradeExecuted

Emitted when two orders match in the order book (a fill).

```json
{
  "event_type": "TradeExecuted",
  "timestamp": "2026-02-18T14:30:00.123789Z",
  "data": {
    "trade_id": "660e8400-e29b-41d4-a716-446655440001",
    "symbol": "AAPL",
    "price": 185.50,
    "quantity": 100,
    "buyer_order_id": "550e8400-e29b-41d4-a716-446655440000",
    "seller_order_id": "770e8400-e29b-41d4-a716-446655440002",
    "buyer_agent_id": "retail-0042",
    "seller_agent_id": "mm-003",
    "aggressor_side": "Buy"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `trade_id` | UUID v4 | Unique trade identifier |
| `symbol` | string | Stock ticker |
| `price` | float | Execution price |
| `quantity` | int | Shares traded |
| `buyer_order_id` | UUID v4 | Buy-side order ID |
| `seller_order_id` | UUID v4 | Sell-side order ID |
| `buyer_agent_id` | string | Buyer's agent identifier |
| `seller_agent_id` | string | Seller's agent identifier |
| `aggressor_side` | enum | `Buy` or `Sell` — which side initiated the match |

#### 2.2.3 QuoteUpdate

Emitted when the best bid or ask price changes.

```json
{
  "event_type": "QuoteUpdate",
  "timestamp": "2026-02-18T14:30:00.124000Z",
  "data": {
    "symbol": "AAPL",
    "bid_price": 185.48,
    "bid_size": 500,
    "ask_price": 185.52,
    "ask_size": 300,
    "spread": 0.04,
    "mid_price": 185.50
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Stock ticker |
| `bid_price` | float | Best bid price |
| `bid_size` | int | Total shares at best bid |
| `ask_price` | float | Best ask price |
| `ask_size` | int | Total shares at best ask |
| `spread` | float | ask_price - bid_price |
| `mid_price` | float | (bid_price + ask_price) / 2 |

#### 2.2.4 OrderBookSnapshot

Periodic full order book state, emitted every `snapshot_interval_ticks`.

```json
{
  "event_type": "OrderBookSnapshot",
  "timestamp": "2026-02-18T14:30:10.000000Z",
  "data": {
    "symbol": "AAPL",
    "bids": [
      {"price": 185.48, "quantity": 500},
      {"price": 185.45, "quantity": 1200},
      {"price": 185.40, "quantity": 800}
    ],
    "asks": [
      {"price": 185.52, "quantity": 300},
      {"price": 185.55, "quantity": 900},
      {"price": 185.60, "quantity": 600}
    ],
    "sequence_number": 42000
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Stock ticker |
| `bids` | array[PriceLevel] | Bid levels sorted by price descending |
| `asks` | array[PriceLevel] | Ask levels sorted by price ascending |
| `sequence_number` | int | Monotonically increasing sequence |

**PriceLevel**: `{ "price": float, "quantity": int }`

#### 2.2.5 TradingHalt

Emitted when a circuit breaker is triggered.

```json
{
  "event_type": "TradingHalt",
  "timestamp": "2026-02-18T14:35:00.000000Z",
  "data": {
    "symbol": "AAPL",
    "reason": "CircuitBreaker",
    "level": 1,
    "trigger_price": 172.05,
    "reference_price": 185.00,
    "decline_pct": 7.0,
    "halt_duration_secs": 300
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Stock ticker |
| `reason` | string | Halt reason (e.g., `CircuitBreaker`) |
| `level` | int | Circuit breaker level (1, 2, or 3) |
| `trigger_price` | float | Price that triggered the halt |
| `reference_price` | float | Reference price (typically opening price) |
| `decline_pct` | float | Percentage decline from reference |
| `halt_duration_secs` | int | Duration of halt in seconds |

**Circuit Breaker Levels**:

| Level | Decline Threshold | Halt Duration |
|-------|-------------------|---------------|
| 1 | 7% | 5 minutes (300s) |
| 2 | 13% | 10 minutes (600s) |
| 3 | 20% | 15 minutes (900s) |

#### 2.2.6 TradingResume

Emitted when trading resumes after a halt.

```json
{
  "event_type": "TradingResume",
  "timestamp": "2026-02-18T14:40:00.000000Z",
  "data": {
    "symbol": "AAPL",
    "halted_at": "2026-02-18T14:35:00.000000Z",
    "halt_duration_secs": 300,
    "level": 1
  }
}
```

#### 2.2.7 AgentAction

Logs an agent's decision or significant action.

```json
{
  "event_type": "AgentAction",
  "timestamp": "2026-02-18T14:30:00.122000Z",
  "data": {
    "agent_id": "inst-002",
    "agent_type": "Institutional",
    "action": "PlaceOrder",
    "symbol": "AAPL",
    "details": {
      "side": "Buy",
      "quantity": 5000,
      "strategy": "block_accumulation"
    }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `agent_id` | string | Agent identifier |
| `agent_type` | enum | Agent type (see Section 2.3) |
| `action` | string | Action taken (e.g., PlaceOrder, CancelOrder, AdjustQuote) |
| `symbol` | string | Relevant stock ticker |
| `details` | object | Action-specific metadata |

#### 2.2.8 MarketStats

Periodic market statistics, emitted every `stats_interval_ticks`.

```json
{
  "event_type": "MarketStats",
  "timestamp": "2026-02-18T14:30:10.000000Z",
  "data": {
    "symbol": "AAPL",
    "open": 185.00,
    "high": 186.20,
    "low": 184.50,
    "last": 185.50,
    "volume": 125000,
    "trade_count": 342,
    "vwap": 185.35,
    "turnover": 23168750.0
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Stock ticker |
| `open` | float | Opening price |
| `high` | float | Highest price in window |
| `low` | float | Lowest price in window |
| `last` | float | Most recent trade price |
| `volume` | int | Total shares traded |
| `trade_count` | int | Number of trades executed |
| `vwap` | float | Volume Weighted Average Price |
| `turnover` | float | Total dollar volume |

#### 2.2.9 OrderCancelled

Emitted when an order is removed from the book (by agent or system).

```json
{
  "event_type": "OrderCancelled",
  "timestamp": "2026-02-18T14:30:05.000000Z",
  "data": {
    "order_id": "550e8400-e29b-41d4-a716-446655440000",
    "symbol": "AAPL",
    "side": "Buy",
    "price": 185.50,
    "remaining_quantity": 50,
    "agent_id": "retail-0042",
    "reason": "AgentCancelled"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | UUID v4 | Original order ID |
| `symbol` | string | Stock ticker |
| `side` | enum | `Buy` or `Sell` |
| `price` | float | Order price |
| `remaining_quantity` | int | Unfilled quantity at cancellation |
| `agent_id` | string | Agent that placed the order |
| `reason` | string | Why cancelled (e.g., `AgentCancelled`, `Expired`, `TradingHalt`) |

### 2.3 Agent Types (6 Types)

DE-Stock simulates 6 distinct agent types, each with different trading behaviors:

#### 2.3.1 Retail Traders

| Property | Value |
|----------|-------|
| Default count | 200 |
| Order size | 10–500 shares |
| Order type mix | 70% market, 30% limit |
| Behavior | Random buys and sells with slight momentum bias |
| Frequency | Low to medium (1–5 orders per minute each) |

Retail traders represent individual investors. They place relatively small orders with a mix of market and limit orders. Their collective behavior creates baseline market activity.

#### 2.3.2 Institutional Investors

| Property | Value |
|----------|-------|
| Default count | 5 |
| Order size | 1,000–10,000 shares |
| Order type mix | 90% limit |
| Behavior | Strategic accumulation/distribution over time (TWAP/VWAP-like) |
| Frequency | Low (bursts of activity) |

Institutional investors place large block orders. They try to minimize market impact by splitting orders over time and preferring limit orders. Their activity can shift price levels.

#### 2.3.3 Market Makers

| Property | Value |
|----------|-------|
| Default count | 10 |
| Order size | 100–1,000 shares per side |
| Order type mix | 100% limit |
| Behavior | Continuously quote bid/ask, earn spread, manage inventory |
| Frequency | Very high (continuous quoting) |

Market makers provide liquidity by continuously placing bid and ask orders. They profit from the spread and adjust quotes based on inventory position and market conditions.

#### 2.3.4 HFT Traders

| Property | Value |
|----------|-------|
| Default count | 15 |
| Order size | 50–500 shares |
| Order type mix | 100% limit |
| Behavior | Rapid order placement/cancellation, exploiting small price movements |
| Frequency | Very high (multiple orders per tick) |

HFT traders operate at the highest frequency. They place and cancel orders rapidly, looking for small per-trade profits. They contribute significant order volume but often cancel before execution.

#### 2.3.5 Noise Traders

| Property | Value |
|----------|-------|
| Default count | 20 |
| Order size | 10–200 shares |
| Order type mix | 50% market, 50% limit |
| Behavior | Random trades with no directional conviction |
| Frequency | Medium |

Noise traders create price movement through random trading. They don't follow any strategy — their orders add randomness and prevent prices from becoming too stable.

#### 2.3.6 Informed Traders

| Property | Value |
|----------|-------|
| Default count | 8 |
| Order size | 200–2,000 shares |
| Order type mix | 80% limit |
| Behavior | Trades with directional conviction based on simulated "information" |
| Frequency | Low to medium (trades in bursts when "informed") |

Informed traders have directional conviction — they believe the price should move in a specific direction. They create price trends and momentum. Other agents may detect and follow their activity.

### 2.4 Market Mechanics

#### 2.4.1 Order Book (Limit Order Book with Price-Time Priority)

The matching engine implements a standard limit order book:

1. **Price Priority**: Orders at better prices execute first (higher bid, lower ask)
2. **Time Priority**: At the same price, earlier orders execute first (FIFO)
3. **Market Orders**: Execute immediately against the best available price
4. **Limit Orders**: Rest on the book until matched or cancelled
5. **Partial Fills**: Large orders can be partially filled across multiple price levels

```
Order Book for AAPL:

    Asks (Sell side)                      Bids (Buy side)
    ------------------                    ------------------
    185.60  |  600 shares                 185.48  |  500 shares
    185.55  |  900 shares                 185.45  |  1200 shares
    185.52  |  300 shares  <-- Best Ask   185.40  |  800 shares
                          Spread: $0.04
                           Best Bid -->

    Incoming BUY MARKET order for 400 shares:
    → Fills 300 @ 185.52 (exhausts best ask)
    → Fills 100 @ 185.55 (partial fill at next level)
    → New best ask: 185.55 (800 remaining)
    → QuoteUpdate emitted
    → TradeExecuted emitted (x2)
```

#### 2.4.2 Circuit Breakers

Circuit breakers halt trading for a symbol when the price drops significantly from a reference price (typically the opening price of the session):

| Level | Decline | Halt Duration | Behavior |
|-------|---------|---------------|----------|
| **Level 1** | 7% | 5 minutes | All new orders rejected; existing orders remain |
| **Level 2** | 13% | 10 minutes | All new orders rejected; existing orders cancelled |
| **Level 3** | 20% | 15 minutes | Full trading halt; order book cleared |

When a circuit breaker triggers:
1. `TradingHalt` event emitted with level, trigger price, and halt duration
2. No new orders accepted for that symbol during halt
3. After halt expires, `TradingResume` event emitted
4. Normal trading resumes

#### 2.4.3 Price Discovery

Unlike a simple GBM price simulation, DE-Stock prices emerge from supply and demand:

1. Agents independently decide to buy or sell based on their strategies
2. Orders flow into the limit order book
3. The matching engine executes trades at the price where supply meets demand
4. QuoteUpdate events reflect the resulting best bid/ask
5. Market maker quoting provides continuous two-sided liquidity
6. Informed trader activity creates directional price trends
7. Noise trader activity adds realistic randomness

This produces emergent price movements that are more realistic than a random walk.

### 2.5 Configuration

DE-Stock is configured via a TOML file. The project maintains two configurations:

#### 2.5.1 Production Configuration (`config/default.toml`)

```toml
# DE-Stock Exchange Simulator Configuration (Production)

[server]
websocket_addr = "0.0.0.0"    # Bind all interfaces (Docker networking)
websocket_port = 8765

[simulation]
tick_interval_ms = 100          # 10 ticks/second
snapshot_interval_ticks = 100   # Order book snapshot every 10 seconds
stats_interval_ticks = 100      # Market stats every 10 seconds

[agents]
retail_traders = 200
institutional_investors = 5
market_makers = 10
hft_traders = 15
noise_traders = 20
informed_traders = 8

[data_quality]
enabled = false                 # Enable later for pipeline testing
duplicate_rate = 0.0
out_of_order_rate = 0.0
missing_data_rate = 0.0

# === Technology Sector ===

[[symbols]]
ticker = "AAPL"
name = "Apple Inc."
initial_price = 185.0
volatility = 0.25
market_cap_category = "large"

[[symbols]]
ticker = "GOOGL"
name = "Alphabet Inc."
initial_price = 142.0
volatility = 0.28
market_cap_category = "large"

[[symbols]]
ticker = "MSFT"
name = "Microsoft Corp."
initial_price = 415.0
volatility = 0.20
market_cap_category = "large"

[[symbols]]
ticker = "NVDA"
name = "NVIDIA Corporation"
initial_price = 450.0
volatility = 0.35
market_cap_category = "large"

[[symbols]]
ticker = "META"
name = "Meta Platforms Inc."
initial_price = 380.0
volatility = 0.32
market_cap_category = "large"

[[symbols]]
ticker = "TSLA"
name = "Tesla Inc."
initial_price = 250.0
volatility = 0.45
market_cap_category = "large"

[[symbols]]
ticker = "AMZN"
name = "Amazon.com Inc."
initial_price = 178.0
volatility = 0.28
market_cap_category = "large"

[[symbols]]
ticker = "CRM"
name = "Salesforce Inc."
initial_price = 270.0
volatility = 0.30
market_cap_category = "large"

# === Finance Sector ===

[[symbols]]
ticker = "JPM"
name = "JPMorgan Chase & Co."
initial_price = 195.0
volatility = 0.22
market_cap_category = "large"

[[symbols]]
ticker = "BAC"
name = "Bank of America Corp."
initial_price = 35.0
volatility = 0.28
market_cap_category = "large"

[[symbols]]
ticker = "GS"
name = "Goldman Sachs Group"
initial_price = 385.0
volatility = 0.25
market_cap_category = "large"

[[symbols]]
ticker = "V"
name = "Visa Inc."
initial_price = 275.0
volatility = 0.18
market_cap_category = "large"

# === Healthcare Sector ===

[[symbols]]
ticker = "JNJ"
name = "Johnson & Johnson"
initial_price = 155.0
volatility = 0.15
market_cap_category = "large"

[[symbols]]
ticker = "UNH"
name = "UnitedHealth Group"
initial_price = 520.0
volatility = 0.22
market_cap_category = "large"

[[symbols]]
ticker = "PFE"
name = "Pfizer Inc."
initial_price = 28.0
volatility = 0.30
market_cap_category = "large"

# === Consumer Sector ===

[[symbols]]
ticker = "WMT"
name = "Walmart Inc."
initial_price = 165.0
volatility = 0.15
market_cap_category = "large"

[[symbols]]
ticker = "KO"
name = "Coca-Cola Company"
initial_price = 60.0
volatility = 0.12
market_cap_category = "large"

[[symbols]]
ticker = "MCD"
name = "McDonald's Corp."
initial_price = 290.0
volatility = 0.18
market_cap_category = "large"

# === Energy Sector ===

[[symbols]]
ticker = "XOM"
name = "Exxon Mobil Corp."
initial_price = 105.0
volatility = 0.25
market_cap_category = "large"

[[symbols]]
ticker = "CVX"
name = "Chevron Corp."
initial_price = 150.0
volatility = 0.22
market_cap_category = "large"

# === Mid-Cap / High Volatility ===

[[symbols]]
ticker = "SMCI"
name = "Super Micro Computer"
initial_price = 50.0
volatility = 0.60
market_cap_category = "mid"

[[symbols]]
ticker = "MSTR"
name = "MicroStrategy Inc."
initial_price = 180.0
volatility = 0.55
market_cap_category = "mid"

[[symbols]]
ticker = "PLTR"
name = "Palantir Technologies"
initial_price = 22.0
volatility = 0.50
market_cap_category = "mid"

[[symbols]]
ticker = "RIVN"
name = "Rivian Automotive"
initial_price = 18.0
volatility = 0.55
market_cap_category = "mid"
```

That gives us 24 symbols across 5 sectors with a mix of large-cap (low volatility) and mid-cap (high volatility) stocks.

#### 2.5.2 Test Configuration (`config/test.toml`)

```toml
# DE-Stock Exchange Simulator Configuration (Testing)
# Minimal config: 2 symbols, fewer agents, faster ticks

[server]
websocket_addr = "0.0.0.0"
websocket_port = 8765

[simulation]
tick_interval_ms = 50           # Faster for testing
snapshot_interval_ticks = 20    # Snapshot every 1 second
stats_interval_ticks = 20       # Stats every 1 second

[agents]
retail_traders = 20
institutional_investors = 1
market_makers = 2
hft_traders = 3
noise_traders = 5
informed_traders = 2

[data_quality]
enabled = false
duplicate_rate = 0.0
out_of_order_rate = 0.0
missing_data_rate = 0.0

[[symbols]]
ticker = "AAPL"
name = "Apple Inc."
initial_price = 185.0
volatility = 0.25
market_cap_category = "large"

[[symbols]]
ticker = "TSLA"
name = "Tesla Inc."
initial_price = 250.0
volatility = 0.45
market_cap_category = "large"
```

### 2.6 Data Quality Simulation

DE-Stock includes a configurable data quality simulation layer for testing downstream data pipelines. When enabled, it introduces realistic data quality issues:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `duplicate_rate` | 0.01 | 1% chance of duplicating an event |
| `out_of_order_rate` | 0.02 | 2% chance of sending events out of timestamp order |
| `missing_data_rate` | 0.01 | 1% chance of dropping an event |

These are intentionally kept **disabled** (`enabled = false`) during normal operation and only turned on when testing data quality validation in the pipeline:

```toml
[data_quality]
enabled = true              # Enable for pipeline testing
duplicate_rate = 0.01       # 1% duplicates
out_of_order_rate = 0.02    # 2% out-of-order
missing_data_rate = 0.01    # 1% missing events
```

This allows testing:
- **Deduplication logic** in the Kafka bridge and Flink jobs
- **Watermark and late-event handling** in Flink
- **Missing data detection** in Great Expectations checks
- **Data reconciliation** between layers

### 2.7 Docker Setup

DE-Stock is built from source using a multi-stage Docker build:

```dockerfile
# services/de-stock/Dockerfile

# Stage 1: Build the Rust binary
FROM rust:1.75-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y git pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/dileepbapat/mock-stock.git .
RUN cargo build --release

# Stage 2: Runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/de-stock /usr/local/bin/
ENTRYPOINT ["de-stock"]
CMD ["/etc/de-stock/default.toml"]
```

The TOML config file is **mounted** into the container (not baked in), allowing runtime configuration changes:

```yaml
# In docker-compose.services.yml
de-stock:
  build:
    context: ../../services/de-stock
    dockerfile: Dockerfile
  container_name: de-stock
  ports:
    - "8765:8765"
  volumes:
    - ../../services/de-stock/config/default.toml:/etc/de-stock/default.toml:ro
  healthcheck:
    test: ["CMD-SHELL", "echo | nc -w1 localhost 8765 || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 5
    start_period: 30s
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
|-- Dockerfile
|-- src/
|   |-- kafka_bridge/
|       |-- __init__.py
|       |-- __main__.py              # Entry point
|       |-- bridge.py                # Main bridge orchestrator
|       |-- websocket_client.py      # WebSocket consumer
|       |-- kafka_producer.py        # Kafka producer wrapper
|       |-- message_router.py        # Routes 9 event types to 8 topics
|       |-- serialization/
|       |   |-- __init__.py
|       |   |-- avro_serializer.py   # Avro serialization with Schema Registry
|       |-- validation/
|       |   |-- __init__.py
|       |   |-- message_validator.py # Pydantic model validation
|       |-- error_handling/
|       |   |-- __init__.py
|       |   |-- dlq.py              # Dead letter queue handler
|       |   |-- retry.py            # Retry with backoff
|       |-- metrics/
|       |   |-- __init__.py
|       |   |-- prometheus.py        # Prometheus metrics
|       |-- config/
|       |   |-- __init__.py
|       |   |-- settings.py          # Pydantic settings
|-- tests/
|   |-- __init__.py
|   |-- test_bridge.py
|   |-- test_websocket_client.py
|   |-- test_kafka_producer.py
|   |-- test_message_router.py
|   |-- test_message_validator.py
|   |-- conftest.py
```

### 3.2 Message Router (9 Event Types → 8 Topics)

The bridge routes each DE-Stock event type to the appropriate Kafka topic:

```python
# message_router.py
from de_common.models.enums import AgentType

class MessageRouter:
    """Routes DE-Stock events to the correct Kafka topic with appropriate partition key."""

    TOPIC_MAP = {
        # Order events → raw.orders (keyed by symbol)
        "OrderPlaced": "raw.orders",
        "OrderCancelled": "raw.orders",

        # Trade events → raw.trades (keyed by symbol)
        "TradeExecuted": "raw.trades",

        # Quote events → raw.quotes (keyed by symbol)
        "QuoteUpdate": "raw.quotes",

        # Order book snapshots → raw.orderbook-snapshots (keyed by symbol)
        "OrderBookSnapshot": "raw.orderbook-snapshots",

        # Market statistics → raw.market-stats (keyed by symbol)
        "MarketStats": "raw.market-stats",

        # Trading halts and resumes → raw.trading-halts (keyed by symbol)
        "TradingHalt": "raw.trading-halts",
        "TradingResume": "raw.trading-halts",

        # Agent actions → raw.agent-actions (keyed by agent_id)
        "AgentAction": "raw.agent-actions",
    }

    def route(self, event_type: str, data: dict) -> tuple[str, str]:
        """
        Returns (topic, partition_key) for a given event.

        Partition key is:
        - symbol for most event types (guarantees per-symbol ordering)
        - agent_id for AgentAction events (guarantees per-agent ordering)
        """
        topic = self.TOPIC_MAP.get(event_type)
        if topic is None:
            raise ValueError(f"Unknown event type: {event_type}")

        if event_type == "AgentAction":
            partition_key = data.get("agent_id", "unknown")
        else:
            partition_key = data.get("symbol", "UNKNOWN")

        return topic, partition_key
```

**Partitioning rationale**:
- **Symbol-keyed topics**: All events for AAPL land on the same partition, ensuring per-symbol ordering. With 24 symbols across 12 partitions, each partition handles ~2 symbols.
- **Agent-keyed topic**: AgentAction events are keyed by `agent_id` because downstream analysis is per-agent rather than per-symbol.

### 3.3 WebSocket Client

```python
# websocket_client.py
import asyncio
import logging
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

class WebSocketConsumer:
    """
    Connects to the DE-Stock WebSocket server and yields messages.
    Implements reconnection with exponential backoff.
    """

    def __init__(
        self,
        uri: str = "ws://de-stock:8765",
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
                    self._retry_count = 0

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

### 3.4 Kafka Producer Configuration

```python
# kafka_producer.py
import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logger = logging.getLogger(__name__)

class KafkaEventProducer:
    """
    Kafka producer configured for reliable, ordered delivery of DE-Stock events.
    Uses Avro serialization with Schema Registry.
    """

    PRODUCER_CONFIG = {
        "bootstrap.servers": "kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092",
        "client.id": "de-stock-bridge-producer",

        # Durability: wait for all in-sync replicas
        "acks": "all",

        # Idempotent producer: exactly-once within a partition
        "enable.idempotence": True,

        # Retries (idempotent producer requires retries > 0)
        "retries": 2147483647,
        "max.in.flight.requests.per.connection": 5,

        # Batching for throughput
        "batch.size": 65536,          # 64 KB
        "linger.ms": 10,
        "batch.num.messages": 500,

        # Compression
        "compression.type": "snappy",

        # Buffer
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 1048576,  # 1 GB

        # Timeouts
        "delivery.timeout.ms": 120000,
        "request.timeout.ms": 30000,

        # Monitoring
        "statistics.interval.ms": 60000,
    }

    def __init__(self, config_overrides: dict | None = None):
        config = {**self.PRODUCER_CONFIG, **(config_overrides or {})}
        self.producer = Producer(config)

        self.schema_registry = SchemaRegistryClient({
            "url": "http://schema-registry:8081",
        })

        self._serializers: dict[str, AvroSerializer] = {}
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
        Key is the partition key (symbol or agent_id).
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
        self.producer.poll(0)

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            logger.error(f"Delivery failed: topic={msg.topic()}, key={msg.key()}, error={err}")
            self._messages_failed += 1
        else:
            self._messages_produced += 1

    def flush(self, timeout: float = 30.0) -> int:
        return self.producer.flush(timeout)
```

### 3.5 Error Handling

#### 3.5.1 WebSocket Reconnection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_retries` | 10 | Maximum reconnection attempts |
| `base_backoff` | 1.0s | Initial backoff duration |
| `max_backoff` | 60.0s | Maximum backoff cap |
| Jitter | 10% | Random jitter to prevent thundering herd |

#### 3.5.2 Kafka Delivery Failure Handling

```python
# error_handling/retry.py
class DeliveryRetryHandler:
    """Handles Kafka delivery failures with configurable retry policy."""

    def __init__(self, max_retries: int = 3, dlq_producer: "DLQProducer" = None):
        self.max_retries = max_retries
        self.dlq_producer = dlq_producer
        self._retry_counts: dict[str, int] = {}

    def handle_failure(self, message_id: str, topic: str, key: str, value: dict, error: str):
        retries = self._retry_counts.get(message_id, 0)

        if retries < self.max_retries:
            self._retry_counts[message_id] = retries + 1
            return "RETRY"
        else:
            if self.dlq_producer:
                self.dlq_producer.send_to_dlq(topic, key, value, error)
            del self._retry_counts[message_id]
            return "DLQ"
```

#### 3.5.3 Dead Letter Queue

```python
# error_handling/dlq.py
import json
from datetime import datetime, timezone

class DLQProducer:
    """Sends failed messages to dlq.raw.failed with error metadata."""

    DLQ_TOPIC = "dlq.raw.failed"

    def __init__(self, producer):
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
            "failed_at": datetime.now(timezone.utc).isoformat(),
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
    "Total messages received from DE-Stock WebSocket",
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
    ["topic", "event_type"],
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
    ["original_topic", "event_type"],
)

# Validation metrics
VALIDATION_FAILURES = Counter(
    "bridge_validation_failures_total",
    "Total messages that failed validation",
    ["event_type", "reason"],
)

def start_metrics_server(port: int = 9090) -> None:
    start_http_server(port)
```

### 3.7 Bridge Dependencies

```toml
# services/kafka-bridge/pyproject.toml
[project]
name = "kafka-bridge"
version = "0.1.0"
description = "WebSocket to Kafka bridge for DE-Stock event ingestion"
requires-python = ">=3.11"

dependencies = [
    "websockets>=13.0,<14.0",
    "confluent-kafka[avro]>=2.3,<3.0",
    "pydantic>=2.5,<3.0",
    "pydantic-settings>=2.1,<3.0",
    "prometheus-client>=0.20,<1.0",
    "de-common",                        # Shared library with Pydantic models
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

We use **KRaft mode** (Kafka Raft) to eliminate the ZooKeeper dependency. Each broker serves as both controller and broker.

```
+---------------+    +---------------+    +---------------+
| Broker 1      |    | Broker 2      |    | Broker 3      |
| :9092 (ext)   |    | :9093 (ext)   |    | :9094 (ext)   |
| :29092 (int)  |    | :29092 (int)  |    | :29092 (int)  |
| :29093 (ctrl) |    | :29093 (ctrl) |    | :29093 (ctrl) |
| (Ctrl+Broker) |    | (Ctrl+Broker) |    | (Ctrl+Broker) |
+---------------+    +---------------+    +---------------+
      |                    |                    |
      +---------+----------+---------+----------+
                |                    |
         +------+------+     +------+------+
         | Schema      |     | Kafka UI    |
         | Registry    |     | :8080       |
         | :8081       |     |             |
         +-------------+     +-------------+
```

**Listener configuration per broker**:
- `PLAINTEXT` (29092): Inter-broker communication
- `CONTROLLER` (29093): KRaft controller communication
- `EXTERNAL` (9092/9093/9094): Client access from host machine

### 4.2 Topic Configuration (8 Topics)

| Topic | Partitions | Replication | Retention | Min ISR | Key | Events |
|-------|-----------|-------------|-----------|---------|-----|--------|
| `raw.orders` | 12 | 3 | 7 days | 2 | symbol | OrderPlaced, OrderCancelled |
| `raw.trades` | 12 | 3 | 7 days | 2 | symbol | TradeExecuted |
| `raw.quotes` | 12 | 3 | 7 days | 2 | symbol | QuoteUpdate |
| `raw.orderbook-snapshots` | 6 | 3 | 3 days | 2 | symbol | OrderBookSnapshot |
| `raw.market-stats` | 6 | 3 | 30 days | 2 | symbol | MarketStats |
| `raw.trading-halts` | 3 | 3 | 30 days | 2 | symbol | TradingHalt, TradingResume |
| `raw.agent-actions` | 6 | 3 | 3 days | 2 | agent_id | AgentAction |
| `dlq.raw.failed` | 3 | 3 | 90 days | 2 | — | Failed messages |

**Partition count rationale**:
- **12 partitions** for high-volume topics (orders, trades, quotes) — divisible by 1, 2, 3, 4, 6, 12 for flexible consumer scaling
- **6 partitions** for medium-volume topics (snapshots, stats, agent actions)
- **3 partitions** for low-volume topics (halts, DLQ)

**Retention rationale**:
- **7 days** for raw event data — sufficient for reprocessing
- **3 days** for snapshots and agent actions — high volume, lower reprocessing value
- **30 days** for stats and halts — used for analytics, infrequent events
- **90 days** for DLQ — allows time for investigation and reprocessing

### 4.3 Topic Initialization Script

```bash
# Embedded in kafka-init container
kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.orders --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.trades --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.quotes --partitions 12 --replication-factor 3 \
  --config retention.ms=604800000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.orderbook-snapshots --partitions 6 --replication-factor 3 \
  --config retention.ms=259200000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.market-stats --partitions 6 --replication-factor 3 \
  --config retention.ms=2592000000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.trading-halts --partitions 3 --replication-factor 3 \
  --config retention.ms=2592000000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic raw.agent-actions --partitions 6 --replication-factor 3 \
  --config retention.ms=259200000 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka-broker-1:29092 --create --if-not-exists \
  --topic dlq.raw.failed --partitions 3 --replication-factor 3 \
  --config retention.ms=7776000000 --config min.insync.replicas=2
```

### 4.4 Schema Registry Configuration

**Compatibility Mode**: BACKWARD (default)

Subject naming convention follows **TopicNameStrategy**:

| Topic | Value Subject | Key Subject |
|-------|--------------|-------------|
| `raw.orders` | `raw.orders-value` | `raw.orders-key` |
| `raw.trades` | `raw.trades-value` | `raw.trades-key` |
| `raw.quotes` | `raw.quotes-value` | `raw.quotes-key` |
| `raw.orderbook-snapshots` | `raw.orderbook-snapshots-value` | `raw.orderbook-snapshots-key` |
| `raw.market-stats` | `raw.market-stats-value` | `raw.market-stats-key` |
| `raw.trading-halts` | `raw.trading-halts-value` | `raw.trading-halts-key` |
| `raw.agent-actions` | `raw.agent-actions-value` | `raw.agent-actions-key` |

Note: `raw.orders` uses a **union schema** since it carries both `OrderPlaced` and `OrderCancelled` events. Similarly, `raw.trading-halts` carries both `TradingHalt` and `TradingResume`.

### 4.5 Kafka UI

Kafka UI (provectuslabs/kafka-ui) provides a web interface at http://localhost:8080 for:
- Viewing broker status and cluster health
- Browsing topics, partitions, and consumer groups
- Inspecting messages (with Avro deserialization via Schema Registry)
- Monitoring consumer lag

---

## 5. Avro Schema Definitions

### 5.1 Shared Enums

```json
// libs/common/schemas/avro/side.avsc
{
  "type": "enum",
  "name": "Side",
  "namespace": "com.destock.events",
  "symbols": ["Buy", "Sell"]
}
```

```json
// libs/common/schemas/avro/order_type.avsc
{
  "type": "enum",
  "name": "OrderType",
  "namespace": "com.destock.events",
  "symbols": ["Market", "Limit"]
}
```

```json
// libs/common/schemas/avro/agent_type.avsc
{
  "type": "enum",
  "name": "AgentType",
  "namespace": "com.destock.events",
  "symbols": ["Retail", "Institutional", "MarketMaker", "HFT", "Noise", "Informed"]
}
```

### 5.2 OrderPlaced Schema

```json
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "com.destock.events",
  "doc": "A new order submitted to the order book by an agent.",
  "fields": [
    {"name": "order_id", "type": "string", "doc": "Unique order identifier (UUID v4)"},
    {"name": "symbol", "type": "string", "doc": "Stock ticker symbol"},
    {"name": "side", "type": "Side", "doc": "Buy or Sell"},
    {"name": "order_type", "type": "OrderType", "doc": "Market or Limit"},
    {"name": "price", "type": "double", "doc": "Order price (0.0 for market orders)"},
    {"name": "quantity", "type": "int", "doc": "Number of shares"},
    {"name": "agent_id", "type": "string", "doc": "Submitting agent identifier"},
    {"name": "agent_type", "type": "AgentType", "doc": "Type of agent"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}, "doc": "Event timestamp UTC"}
  ]
}
```

### 5.3 TradeExecuted Schema

```json
{
  "type": "record",
  "name": "TradeExecuted",
  "namespace": "com.destock.events",
  "doc": "Two orders matched in the order book.",
  "fields": [
    {"name": "trade_id", "type": "string", "doc": "Unique trade identifier (UUID v4)"},
    {"name": "symbol", "type": "string", "doc": "Stock ticker symbol"},
    {"name": "price", "type": "double", "doc": "Execution price"},
    {"name": "quantity", "type": "int", "doc": "Shares traded"},
    {"name": "buyer_order_id", "type": "string", "doc": "Buy-side order ID"},
    {"name": "seller_order_id", "type": "string", "doc": "Sell-side order ID"},
    {"name": "buyer_agent_id", "type": "string", "doc": "Buyer's agent identifier"},
    {"name": "seller_agent_id", "type": "string", "doc": "Seller's agent identifier"},
    {"name": "aggressor_side", "type": "Side", "doc": "Which side initiated the match"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}, "doc": "Event timestamp UTC"}
  ]
}
```

### 5.4 QuoteUpdate Schema

```json
{
  "type": "record",
  "name": "QuoteUpdate",
  "namespace": "com.destock.events",
  "doc": "Best bid/ask price change.",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "bid_price", "type": "double"},
    {"name": "bid_size", "type": "int"},
    {"name": "ask_price", "type": "double"},
    {"name": "ask_size", "type": "int"},
    {"name": "spread", "type": "double"},
    {"name": "mid_price", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.5 OrderBookSnapshot Schema

```json
{
  "type": "record",
  "name": "OrderBookSnapshot",
  "namespace": "com.destock.events",
  "doc": "Full order book state at a point in time.",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "bids", "type": {"type": "array", "items": {
      "type": "record", "name": "PriceLevel", "fields": [
        {"name": "price", "type": "double"},
        {"name": "quantity", "type": "int"}
      ]
    }}},
    {"name": "asks", "type": {"type": "array", "items": "PriceLevel"}},
    {"name": "sequence_number", "type": "long"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.6 MarketStats Schema

```json
{
  "type": "record",
  "name": "MarketStats",
  "namespace": "com.destock.events",
  "doc": "Periodic market statistics for a symbol.",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "open", "type": "double"},
    {"name": "high", "type": "double"},
    {"name": "low", "type": "double"},
    {"name": "last", "type": "double"},
    {"name": "volume", "type": "long"},
    {"name": "trade_count", "type": "int"},
    {"name": "vwap", "type": "double"},
    {"name": "turnover", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.7 TradingHalt Schema

```json
{
  "type": "record",
  "name": "TradingHalt",
  "namespace": "com.destock.events",
  "doc": "Circuit breaker triggered — trading halted.",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "reason", "type": "string"},
    {"name": "level", "type": "int"},
    {"name": "trigger_price", "type": "double"},
    {"name": "reference_price", "type": "double"},
    {"name": "decline_pct", "type": "double"},
    {"name": "halt_duration_secs", "type": "int"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.8 TradingResume Schema

```json
{
  "type": "record",
  "name": "TradingResume",
  "namespace": "com.destock.events",
  "doc": "Trading resumed after a halt.",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "halted_at", "type": "string"},
    {"name": "halt_duration_secs", "type": "int"},
    {"name": "level", "type": "int"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.9 AgentAction Schema

```json
{
  "type": "record",
  "name": "AgentAction",
  "namespace": "com.destock.events",
  "doc": "Agent decision or action logged.",
  "fields": [
    {"name": "agent_id", "type": "string"},
    {"name": "agent_type", "type": "AgentType"},
    {"name": "action", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "details", "type": {"type": "map", "values": "string"}},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.10 OrderCancelled Schema

```json
{
  "type": "record",
  "name": "OrderCancelled",
  "namespace": "com.destock.events",
  "doc": "An order removed from the order book.",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "side", "type": "Side"},
    {"name": "price", "type": "double"},
    {"name": "remaining_quantity", "type": "int"},
    {"name": "agent_id", "type": "string"},
    {"name": "reason", "type": "string"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-micros"}}
  ]
}
```

### 5.11 Schema Evolution Rules

| Change | Compatibility | Action Required |
|--------|---------------|-----------------|
| Add optional field with default | BACKWARD | Register new schema version |
| Remove optional field | BACKWARD | Register new schema version |
| Add required field without default | BREAKING | Requires new topic or migration |
| Change field type | BREAKING | Requires new topic or migration |
| Rename field | BREAKING | Requires new topic or migration |
| Add new enum value | BACKWARD | Register new schema version |
| Remove enum value | BREAKING | Requires careful migration |

### 5.12 Schema Registration Script

```bash
#!/bin/bash
# infrastructure/scripts/register-schemas.sh

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="libs/common/schemas/avro"

register_schema() {
  local subject="$1"
  local schema_file="$2"

  echo "Registering schema: ${subject} from ${schema_file}"
  schema_json=$(cat "${schema_file}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')

  curl -s -X POST "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\": \"AVRO\", \"schema\": ${schema_json}}" | python3 -m json.tool

  echo ""
}

# Register all value schemas
register_schema "raw.orders-value" "${SCHEMA_DIR}/order_placed.avsc"
register_schema "raw.trades-value" "${SCHEMA_DIR}/trade_executed.avsc"
register_schema "raw.quotes-value" "${SCHEMA_DIR}/quote_update.avsc"
register_schema "raw.orderbook-snapshots-value" "${SCHEMA_DIR}/orderbook_snapshot.avsc"
register_schema "raw.market-stats-value" "${SCHEMA_DIR}/market_stats.avsc"
register_schema "raw.trading-halts-value" "${SCHEMA_DIR}/trading_halt.avsc"
register_schema "raw.agent-actions-value" "${SCHEMA_DIR}/agent_action.avsc"

# Set compatibility mode
for subject in \
  "raw.orders-value" "raw.trades-value" "raw.quotes-value" \
  "raw.orderbook-snapshots-value" "raw.market-stats-value" \
  "raw.trading-halts-value" "raw.agent-actions-value"; do
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

### 6.1 Producer-Consumer Contract (DE-Stock → Bridge)

**Transport**: WebSocket (`ws://de-stock:8765`)

**Message Envelope**:

```json
{
  "event_type": "<string>",       // Required. One of 9 event types
  "timestamp": "<ISO 8601 UTC>",  // Required. Server-side event timestamp
  "data": { ... }                 // Required. Event-specific fields
}
```

**Contract Rules**:
1. The `event_type` field MUST be one of: `OrderPlaced`, `TradeExecuted`, `QuoteUpdate`, `OrderBookSnapshot`, `TradingHalt`, `TradingResume`, `AgentAction`, `MarketStats`, `OrderCancelled`.
2. The `timestamp` field MUST be in ISO 8601 format, UTC timezone.
3. The `data` object MUST contain all required fields for the given event type.
4. Prices are transmitted as JSON numbers (float64).
5. UUIDs are lowercase hex with dashes.
6. Field names use `snake_case`.

### 6.2 Bridge-Kafka Contract

**Serialization**: Avro (via Schema Registry)
**Key**: UTF-8 string (symbol or agent_id)
**Headers**:

| Header | Description |
|--------|-------------|
| `event_type` | Original DE-Stock event type |
| `source` | `de-stock` |
| `bridge_timestamp` | Bridge processing timestamp |

### 6.3 SLA Definitions

| Metric | SLA Target | Measurement Point |
|--------|-----------|-------------------|
| **End-to-end latency** | < 500ms (p99) | DE-Stock timestamp to Kafka produce ack |
| **Throughput** | >= 1,000 msgs/sec | Kafka produce rate (sustained) |
| **Availability** | 99.9% | Kafka cluster uptime |
| **Data loss** | 0 messages | `acks=all` + `min.insync.replicas=2` |
| **Schema validation** | 100% of messages | Bridge validates before producing |
| **WebSocket reconnection** | < 120s | Maximum time to re-establish connection |
| **DLQ processing** | Review within 24hrs | Manual triage of DLQ messages |

---

## 7. Implementation Steps

### Phase 1: Infrastructure + DE-Stock (this plan)

| Step | Path | Description |
|------|------|-------------|
| 1.1 | `services/de-stock/Dockerfile` | Multi-stage Rust build from source |
| 1.2 | `services/de-stock/config/default.toml` | Production config (24 symbols, 258 agents) |
| 1.3 | `services/de-stock/config/test.toml` | Minimal test config (2 symbols, 33 agents) |
| 1.4 | `libs/common/pyproject.toml` | Shared library package |
| 1.5 | `libs/common/src/de_common/__init__.py` | Package init |
| 1.6 | `libs/common/src/de_common/models/__init__.py` | Models package |
| 1.7 | `libs/common/src/de_common/models/enums.py` | Side, OrderType, AgentType, CircuitBreakerLevel |
| 1.8 | `libs/common/src/de_common/models/order.py` | OrderPlacedEvent, OrderCancelledEvent |
| 1.9 | `libs/common/src/de_common/models/trade.py` | TradeExecutedEvent |
| 1.10 | `libs/common/src/de_common/models/quote.py` | QuoteUpdateEvent |
| 1.11 | `libs/common/src/de_common/models/orderbook.py` | OrderBookSnapshot, PriceLevel |
| 1.12 | `libs/common/src/de_common/models/market_stats.py` | MarketStatsEvent |
| 1.13 | `libs/common/src/de_common/models/trading_halt.py` | TradingHaltEvent, TradingResumeEvent |
| 1.14 | `libs/common/src/de_common/models/agent_action.py` | AgentActionEvent |
| 1.15 | `libs/common/src/de_common/models/events.py` | MarketEvent discriminated union |
| 1.16 | `infrastructure/docker-compose/docker-compose.kafka.yml` | 3 KRaft brokers, Schema Registry, Kafka UI, init |
| 1.17 | `infrastructure/docker-compose/docker-compose.services.yml` | DE-Stock service |

### Phase 2: Kafka Bridge + Schemas

| Step | Path | Description |
|------|------|-------------|
| 2.1 | `services/kafka-bridge/pyproject.toml` | Bridge package with dependencies |
| 2.2 | `services/kafka-bridge/src/kafka_bridge/__main__.py` | Entry point |
| 2.3 | `services/kafka-bridge/src/kafka_bridge/bridge.py` | Main orchestrator |
| 2.4 | `services/kafka-bridge/src/kafka_bridge/websocket_client.py` | WebSocket consumer |
| 2.5 | `services/kafka-bridge/src/kafka_bridge/kafka_producer.py` | Kafka producer |
| 2.6 | `services/kafka-bridge/src/kafka_bridge/message_router.py` | 9 types → 8 topics |
| 2.7 | `services/kafka-bridge/src/kafka_bridge/validation/message_validator.py` | Pydantic validation |
| 2.8 | `services/kafka-bridge/src/kafka_bridge/serialization/avro_serializer.py` | Avro + Schema Registry |
| 2.9 | `services/kafka-bridge/src/kafka_bridge/error_handling/dlq.py` | DLQ handler |
| 2.10 | `services/kafka-bridge/src/kafka_bridge/error_handling/retry.py` | Retry handler |
| 2.11 | `services/kafka-bridge/src/kafka_bridge/metrics/prometheus.py` | Prometheus metrics |
| 2.12 | `services/kafka-bridge/src/kafka_bridge/config/settings.py` | Pydantic settings |
| 2.13 | `services/kafka-bridge/Dockerfile` | Bridge container image |
| 2.14 | `libs/common/schemas/avro/*.avsc` | Avro schemas for all 9 event types |
| 2.15 | `infrastructure/scripts/register-schemas.sh` | Schema registration script |
| 2.16 | `services/kafka-bridge/tests/` | Unit and integration tests |

### Phase 3: Integration and Verification

| Step | Path | Description |
|------|------|-------------|
| 3.1 | `tests/integration/test_e2e_ingestion.py` | Full pipeline: DE-Stock → WS → Bridge → Kafka |
| 3.2 | `tests/integration/test_schema_registry.py` | Schema registration and evolution tests |
| 3.3 | `tests/load/locustfile.py` | Load testing scenarios |

---

## 8. Testing Strategy

### 8.1 Unit Tests

**Shared Library Tests**:

| Test Area | Test Cases |
|-----------|-----------|
| `Pydantic Models` | All 9 event types deserialize correctly from JSON; required fields enforced; enum validation |
| `MarketEvent union` | Discriminated union correctly identifies event type; round-trip serialization |
| `Enums` | All enum values valid; string serialization matches DE-Stock output |

**Kafka Bridge Tests**:

| Test Area | Test Cases |
|-----------|-----------|
| `MessageRouter` | All 9 event types route to correct topic; correct partition key; unknown type raises error |
| `MessageValidator` | Valid messages pass; missing fields rejected; invalid event types rejected |
| `DLQProducer` | Failed messages include original payload + error metadata |
| `RetryHandler` | Retries up to max; sends to DLQ after exhaustion |
| `PrometheusMetrics` | Counters increment; labels are correct per event type |

### 8.2 Integration Tests

**WebSocket Integration**:
```python
# tests/integration/test_e2e_ingestion.py
import pytest
import asyncio
import json
from websockets.asyncio.client import connect

VALID_EVENT_TYPES = {
    "OrderPlaced", "TradeExecuted", "QuoteUpdate",
    "OrderBookSnapshot", "TradingHalt", "TradingResume",
    "AgentAction", "MarketStats", "OrderCancelled",
}

@pytest.mark.asyncio
async def test_de_stock_streams_events():
    """Connect to DE-Stock WebSocket and verify event format."""
    async with connect("ws://localhost:8765") as ws:
        for _ in range(20):
            message = await asyncio.wait_for(ws.recv(), timeout=5.0)
            data = json.loads(message)
            assert "event_type" in data
            assert "timestamp" in data
            assert "data" in data
            assert data["event_type"] in VALID_EVENT_TYPES


@pytest.mark.asyncio
async def test_de_stock_multiple_event_types():
    """Verify we receive multiple different event types within a time window."""
    event_types_seen = set()
    async with connect("ws://localhost:8765") as ws:
        for _ in range(200):
            message = await asyncio.wait_for(ws.recv(), timeout=5.0)
            data = json.loads(message)
            event_types_seen.add(data["event_type"])
            if len(event_types_seen) >= 4:
                break
    assert len(event_types_seen) >= 4, f"Only saw: {event_types_seen}"
```

**Kafka Integration** (using testcontainers):
```python
@pytest.mark.integration
async def test_bridge_routes_to_correct_topics():
    """
    1. Start Kafka via testcontainers
    2. Connect bridge to DE-Stock + Kafka
    3. Verify messages arrive in correct topics based on event type
    4. Verify partition key matches symbol/agent_id
    """
    # Verify: raw.orders has OrderPlaced and OrderCancelled
    # Verify: raw.trades has TradeExecuted
    # Verify: raw.quotes has QuoteUpdate
    # etc.
```

### 8.3 Load Testing

| Scenario | Rate | Duration | Success Criteria |
|----------|------|----------|-----------------|
| Steady state | 100 msg/s | 10 min | p99 latency < 500ms, 0 messages lost |
| Burst | 5,000 msg/s | 30 sec | No OOM, queue depth recovers within 60s |
| Soak | 50 msg/s | 1 hour | No memory growth > 10%, stable latency |
| Recovery | N/A | N/A | After broker restart, bridge reconnects < 60s |
| Data quality | 100 msg/s | 5 min | With data_quality enabled, DLQ receives expected error rate |

### 8.4 Data Quality Testing

When `data_quality.enabled = true` in DE-Stock config:

| Test | Configuration | Expected Outcome |
|------|--------------|-----------------|
| Duplicate detection | `duplicate_rate = 0.05` | Bridge detects ~5% duplicates, deduplicates or flags them |
| Out-of-order handling | `out_of_order_rate = 0.05` | Flink watermarks handle late events; no data loss |
| Missing data detection | `missing_data_rate = 0.05` | Great Expectations checks detect gaps in sequences |

---

## 9. Configuration

### 9.1 Kafka Bridge Configuration

```python
# config/settings.py
from pydantic_settings import BaseSettings

class KafkaBridgeSettings(BaseSettings):
    """Configuration loaded from environment variables or .env file."""

    # WebSocket source (DE-Stock)
    ws_uri: str = "ws://de-stock:8765"
    ws_max_retries: int = 10
    ws_base_backoff: float = 1.0
    ws_max_backoff: float = 60.0

    # Kafka producer
    kafka_bootstrap_servers: str = "kafka-broker-1:29092,kafka-broker-2:29092,kafka-broker-3:29092"
    kafka_client_id: str = "de-stock-bridge-producer"
    kafka_acks: str = "all"
    kafka_batch_size: int = 65536
    kafka_linger_ms: int = 10
    kafka_compression_type: str = "snappy"
    kafka_enable_idempotence: bool = True

    # Schema Registry
    schema_registry_url: str = "http://schema-registry:8081"

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

### 9.2 Environment-Specific Overrides

Configuration priority (highest wins):
1. Environment variables (prefixed with `KAFKA_BRIDGE_`)
2. `.env` file (loaded by pydantic-settings)
3. Default values in the Settings class

```bash
# environments/dev.env
KAFKA_BRIDGE_WS_URI=ws://localhost:8765
KAFKA_BRIDGE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
KAFKA_BRIDGE_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_BRIDGE_LOG_LEVEL=DEBUG
```

```bash
# environments/load-test.env
KAFKA_BRIDGE_KAFKA_BATCH_SIZE=131072
KAFKA_BRIDGE_KAFKA_LINGER_MS=50
KAFKA_BRIDGE_LOG_LEVEL=WARNING
```

### 9.3 Tunable Parameters Summary

| Parameter | Default | Range / Notes | Impact |
|-----------|---------|---------------|--------|
| DE-Stock `tick_interval_ms` | 100 | 1 - 1000 | Lower = more events/sec, higher CPU |
| DE-Stock `retail_traders` | 200 | 1 - 1000 | More agents = more orders/trades |
| DE-Stock `volatility` | varies | 0.1 - 1.0 | Higher = more price swings, circuit breakers |
| `kafka_batch_size` | 64 KB | 16 KB - 1 MB | Larger = higher throughput, latency |
| `kafka_linger_ms` | 10 | 0 - 100 | Higher = more batching, higher latency |
| `kafka_compression_type` | snappy | none, gzip, snappy, lz4, zstd | snappy: best speed; zstd: best ratio |
| `kafka_acks` | all | 0, 1, all | all: highest durability |
| `ws_base_backoff` | 1.0s | 0.1 - 5.0 | Lower = faster reconnect |
| `data_quality.enabled` | false | true/false | Enable to test pipeline resilience |

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
|       |-- src/de_common/
|       |   |-- __init__.py
|       |   |-- models/
|       |   |   |-- __init__.py
|       |   |   |-- enums.py               # Side, OrderType, AgentType, CircuitBreakerLevel
|       |   |   |-- order.py               # OrderPlacedEvent, OrderCancelledEvent
|       |   |   |-- trade.py               # TradeExecutedEvent
|       |   |   |-- quote.py               # QuoteUpdateEvent
|       |   |   |-- orderbook.py           # OrderBookSnapshot, PriceLevel
|       |   |   |-- market_stats.py        # MarketStatsEvent
|       |   |   |-- trading_halt.py        # TradingHaltEvent, TradingResumeEvent
|       |   |   |-- agent_action.py        # AgentActionEvent
|       |   |   |-- events.py              # MarketEvent discriminated union
|       |   |-- schemas/
|       |       |-- __init__.py
|       |-- schemas/
|           |-- avro/
|               |-- side.avsc
|               |-- order_type.avsc
|               |-- agent_type.avsc
|               |-- order_placed.avsc
|               |-- order_cancelled.avsc
|               |-- trade_executed.avsc
|               |-- quote_update.avsc
|               |-- orderbook_snapshot.avsc
|               |-- market_stats.avsc
|               |-- trading_halt.avsc
|               |-- trading_resume.avsc
|               |-- agent_action.avsc
|-- services/
|   |-- de-stock/
|   |   |-- Dockerfile
|   |   |-- config/
|   |       |-- default.toml
|   |       |-- test.toml
|   |-- kafka-bridge/
|       |-- pyproject.toml
|       |-- Dockerfile
|       |-- src/kafka_bridge/
|       |   |-- __init__.py
|       |   |-- __main__.py
|       |   |-- bridge.py
|       |   |-- websocket_client.py
|       |   |-- kafka_producer.py
|       |   |-- message_router.py
|       |   |-- serialization/
|       |   |-- validation/
|       |   |-- error_handling/
|       |   |-- metrics/
|       |   |-- config/
|       |-- tests/
|-- infrastructure/
|   |-- docker-compose/
|   |   |-- docker-compose.kafka.yml
|   |   |-- docker-compose.services.yml
|   |-- scripts/
|       |-- register-schemas.sh
|-- tests/
|   |-- integration/
|   |   |-- test_e2e_ingestion.py
|   |   |-- test_schema_registry.py
|   |-- load/
|       |-- locustfile.py
|-- Makefile
```

### 10.2 Port Map

| Service | Host Port | Container Port | Purpose |
|---------|-----------|---------------|---------|
| DE-Stock WebSocket | 8765 | 8765 | Trade simulator event stream |
| Kafka Broker 1 | 9092 | 9092 | External client access |
| Kafka Broker 2 | 9093 | 9092 | External client access |
| Kafka Broker 3 | 9094 | 9092 | External client access |
| Schema Registry | 8081 | 8081 | Schema management |
| Kafka UI | 8080 | 8080 | Kafka monitoring UI |
| Kafka Bridge Metrics | 9090 | 9090 | Prometheus metrics |

### 10.3 Event Type → Topic → Key Summary

| Event Type | Kafka Topic | Partition Key | Volume |
|-----------|-------------|---------------|--------|
| OrderPlaced | raw.orders | symbol | High |
| OrderCancelled | raw.orders | symbol | Medium |
| TradeExecuted | raw.trades | symbol | High |
| QuoteUpdate | raw.quotes | symbol | Very High |
| OrderBookSnapshot | raw.orderbook-snapshots | symbol | Low (periodic) |
| MarketStats | raw.market-stats | symbol | Low (periodic) |
| TradingHalt | raw.trading-halts | symbol | Very Low (rare) |
| TradingResume | raw.trading-halts | symbol | Very Low (rare) |
| AgentAction | raw.agent-actions | agent_id | Medium |

### 10.4 Quick Start Sequence

```bash
# 1. Start infrastructure (Kafka + Storage + Flink)
make up-infra

# 2. Start DE-Stock simulator
make up-services

# 3. Verify DE-Stock is streaming
python3 -c "
import asyncio, websockets, json
async def test():
    async with websockets.connect('ws://localhost:8765') as ws:
        for _ in range(5):
            msg = json.loads(await ws.recv())
            print(f'{msg[\"event_type\"]}: {list(msg[\"data\"].keys())[:4]}...')
asyncio.run(test())
"

# 4. (Phase 2) Start the Kafka bridge
make bridge

# 5. Open Kafka UI to monitor
open http://localhost:8080

# 6. Verify messages are flowing in topics
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic raw.trades \
  --from-beginning \
  --max-messages 5
```
