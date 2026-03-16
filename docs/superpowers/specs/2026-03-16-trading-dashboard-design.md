# Trading Dashboard — Design Specification

## Overview

A React + Vite frontend application that consumes the project's GraphQL API to display interactive candlestick charts with MACD technical analysis, real-time trade streaming, and market data. Deployed as `services/trading-dashboard/` alongside existing services.

## Decisions

| Decision | Choice |
|----------|--------|
| Use case | Real-time streaming + historical technical analysis |
| Symbol view | Main detailed chart + left sidebar with watchlist & order book |
| Stack | React 18 + Vite + TypeScript |
| Charting | Lightweight Charts (by TradingView) |
| Layout | Left sidebar (watchlist + order book), main area (candlestick + MACD) |
| Real-time | Append mode — live trades build into the latest candle |
| GraphQL client | Apollo Client with split link (HTTP + WebSocket) |
| MACD | Client-side computation: EMA(12), EMA(26), Signal(9) |
| Docker port | 3002 (nginx production), 5173 (Vite dev) |

## Important: GraphQL Field Names

Strawberry (the Python GraphQL framework used by the API) converts all Python snake_case field names to camelCase on the wire. **All GraphQL documents in the frontend must use camelCase.**

Examples: `trading_date` → `tradingDate`, `open_price` → `openPrice`, `close_price` → `closePrice`, `total_volume` → `totalVolume`, `price_change_pct` → `priceChangePct`, `best_bid_price` → `bestBidPrice`, `trade_id` → `tradeId`, `is_aggressive_buy` → `isAggressiveBuy`, `company_name` → `companyName`.

## Architecture

### Data Sources (GraphQL API at localhost:8000)

All queries that return lists use **Relay-style cursor pagination** (`edges[].node`, `pageInfo`, `totalCount`). The frontend must handle this pagination shape.

**Queries:**

| Query | Purpose | Pagination | Cache/Poll Strategy |
|-------|---------|------------|---------------------|
| `dailySummary(symbol, dateRange, first, after)` | Historical OHLCV candles | `DailySummaryConnection` (default `first: 30`) | Apollo cache, re-fetch on symbol change |
| `symbols(filter, first, after)` | Populate watchlist / symbol picker | `SymbolConnection` (default `first: 50`) | Apollo cache (1hr, matches API cache TTL) |
| `marketOverview(targetDate)` | Top gainers/losers/most active | Not paginated (flat `MarketOverview` type) | Poll every 60s |
| `orderBook(symbol)` | Sidebar best bid/ask + spread | Not paginated (flat `OrderBookSnapshot`) | Poll every 5s |

**Subscriptions:**

| Subscription | Purpose |
|--------------|---------|
| `onNewTrade(symbol)` | Stream trades, aggregate into live-building candle |

No mutations — this is a read-only dashboard. Watchlist is client-side state only (v1).

### Example GraphQL Documents (camelCase)

**Daily Summary (paginated):**
```graphql
query DailySummary($symbol: String!, $dateRange: DateRangeInput!, $first: Int, $after: String) {
  dailySummary(symbol: $symbol, dateRange: $dateRange, first: $first, after: $after) {
    edges {
      node {
        tradingDate
        openPrice
        closePrice
        highPrice
        lowPrice
        totalVolume
        tradeCount
        totalValue
        companyName
        priceChange
        priceChangePct
      }
    }
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
```

**onNewTrade Subscription:**
```graphql
subscription OnNewTrade($symbol: String) {
  onNewTrade(symbol: $symbol) {
    tradeId
    symbol
    price
    quantity
    timestamp
    isAggressiveBuy
  }
}
```

### Data Flow

**Historical (on load + symbol change):**
```
dailySummary query (first: 365 for ~1 year of daily candles)
  → GraphQL API → DuckDB/Iceberg gold.daily_trading_summary
  → Returns: DailySummaryConnection { edges[].node { tradingDate, openPrice, closePrice, ... } }
  → useOhlcvData hook follows pageInfo.hasNextPage/endCursor to fetch all pages
  → useMacd hook computes EMA(12), EMA(26), signal(9), histogram from close prices
  → CandlestickChart renders candles + volume bars
  → MacdChart renders MACD line, signal line, histogram
```

**Real-time (WebSocket subscription):**
```
onNewTrade subscription → GraphQL API → Kafka enriched.trades topic
  → Each trade: { tradeId, symbol, price, quantity, timestamp, isAggressiveBuy }
  → useTradeSubscription aggregates into current candle:
      - Update high if price > current high
      - Update low if price < current low
      - Update close to latest price
      - Accumulate volume
  → Lightweight Charts .update() on last candle (no full re-render)
  → Recalculate latest MACD values
```

**Order book:** `orderBook` query polled every 5s for selected symbol. Note: the API returns only the **best bid/ask** (single level each), not a full depth ladder. The component displays best bid, best ask, spread, mid-price, and depth counts — not a multi-level order book.

**Watchlist:** `marketOverview` query polled every 60s provides `topGainers`, `topLosers`, and `mostActive` lists. For symbols not in those lists, the watchlist shows the last trade price from the `onNewTrade` subscription (updated live) and falls back to a per-symbol `dailySummary` query for initial load:
```graphql
# Watchlist fallback — dateRange is required by the API
query WatchlistPrice($symbol: String!, $today: Date!) {
  dailySummary(symbol: $symbol, dateRange: { start: $today, end: $today }, first: 1) {
    edges { node { closePrice priceChangePct } }
  }
}
```

## Project Structure

```
services/trading-dashboard/
├── public/
├── src/
│   ├── main.tsx                    # App entry
│   ├── App.tsx                     # Layout shell
│   ├── graphql/
│   │   ├── client.ts               # Apollo Client (HTTP + WS split link)
│   │   ├── queries.ts              # dailySummary, symbols, marketOverview, orderBook
│   │   └── subscriptions.ts        # onNewTrade
│   ├── components/
│   │   ├── CandlestickChart.tsx    # Lightweight Charts candlestick + volume
│   │   ├── MacdChart.tsx           # MACD pane (histogram + signal + MACD line)
│   │   ├── Watchlist.tsx           # Symbol list with price/change, click to switch
│   │   ├── OrderBook.tsx           # Bid/ask depth display
│   │   ├── SymbolHeader.tsx        # Symbol name, live price, change, high/low/volume
│   │   └── ConnectionStatus.tsx    # LIVE / CONNECTING / DISCONNECTED badge
│   ├── hooks/
│   │   ├── useTradeSubscription.ts # onNewTrade → candle aggregation
│   │   ├── useOhlcvData.ts         # Fetch historical dailySummary → chart data
│   │   └── useMacd.ts             # Compute MACD from OHLCV close prices
│   ├── utils/
│   │   ├── macd.ts                 # Pure MACD calculation functions
│   │   └── candle.ts               # Trade → candle aggregation logic
│   └── types/
│       └── index.ts                # TypeScript interfaces
├── index.html
├── vite.config.ts
├── tsconfig.json
├── package.json
├── Dockerfile
└── .env
```

## Component Specifications

### CandlestickChart.tsx
- Lightweight Charts `createChart()` with dark theme (background: `#1a1a2e`)
- `chart.addCandlestickSeries()` for OHLCV candles (green up / red down)
- `chart.addHistogramSeries()` for volume bars (lower opacity, secondary price scale)
- Crosshair with price/time tooltip
- `chart.timeScale().fitContent()` on initial load
- Live update via `candleSeries.update(latestCandle)` — no full re-render
- Responsive: re-layout on container resize via `ResizeObserver`

### MacdChart.tsx
- Separate Lightweight Charts instance below the main chart
- Time scale synced with CandlestickChart via `timeScale().subscribeVisibleLogicalRangeChange()`
- 3 series (using v4 API — `chart.addLineSeries()`, `chart.addHistogramSeries()`):
  - MACD line (blue, via `addLineSeries()`)
  - Signal line (orange, via `addLineSeries()`)
  - Histogram (green/red via `addHistogramSeries()`, color based on sign)
- Recalculates on historical data load or live candle update

### Watchlist.tsx
- Vertical list of symbols with last price and daily % change
- Active symbol highlighted with accent border
- Click to switch: triggers re-fetch of `dailySummary` + re-subscribe `onNewTrade`
- Price change data from `marketOverview` query (polled every 60s)
- Color-coded: green (positive), red (negative)

### OrderBook.tsx
- Displays **best bid** (green) and **best ask** (red) — single level each (API limitation)
- Spread and mid-price displayed between bid/ask
- `bidDepth` and `askDepth` counts shown as supplementary info
- Data from `orderBook` query polled every 5s
- Note: The API's `OrderBookSnapshot` only exposes the top-of-book, not a full depth ladder

### SymbolHeader.tsx
- Current symbol ticker, company name, sector
- Live price updates on each trade from subscription
- Daily change ($ and %), session high/low, cumulative volume
- Color-coded: green for positive change, red for negative

### ConnectionStatus.tsx
- `LIVE` (green pulse animation) when WebSocket connected
- `CONNECTING` (yellow) during connection attempt
- `DISCONNECTED` (red) on failure
- Auto-reconnect with exponential backoff

## MACD Calculation

Computed client-side in `utils/macd.ts`:

**Parameters:** EMA(12), EMA(26), Signal EMA(9) — industry standard defaults.

**Algorithm:**
1. MACD Line = EMA(12, close prices) - EMA(26, close prices)
2. Signal Line = EMA(9, MACD Line values)
3. Histogram = MACD Line - Signal Line

**EMA formula:**
```
multiplier = 2 / (period + 1)
ema[0] = SMA(first N prices)  // seed with simple moving average
ema[i] = (price[i] - ema[i-1]) * multiplier + ema[i-1]
```

**Rendering:**
- MACD line: blue
- Signal line: orange
- Histogram: green bars when MACD > Signal (bullish), red when MACD < Signal (bearish)

**Minimum data:** 26 candles required before MACD values are meaningful. Chart shows candles from the start; MACD appears after sufficient data.

**Live update:** On candle close or live price change, recompute only the latest EMA values incrementally.

## Apollo Client Setup

**Split link:**
```
HTTP Link  → http://localhost:8000/graphql  (queries, mutations)
WS Link    → ws://localhost:8000/graphql    (subscriptions, graphql-transport-ws protocol)
```

Apollo automatically routes operations based on type.

**Environment variables:**
```
VITE_GRAPHQL_HTTP_URL=http://localhost:8000/graphql
VITE_GRAPHQL_WS_URL=ws://localhost:8000/graphql
```

## Docker Integration

**Dockerfile:** Multi-stage build (node:20-alpine build → nginx:alpine serve).

**docker-compose.services.yml entry:**
```yaml
trading-dashboard:
  build:
    context: ../../services/trading-dashboard
    dockerfile: Dockerfile
    args:
      VITE_GRAPHQL_HTTP_URL: http://localhost:8000/graphql
      VITE_GRAPHQL_WS_URL: ws://localhost:8000/graphql
  ports:
    - "3002:80"
  depends_on:
    graphql-api:
      condition: service_healthy
```

The Dockerfile must declare `ARG VITE_GRAPHQL_HTTP_URL` and `ARG VITE_GRAPHQL_WS_URL` before the build step so Vite bakes them into the static bundle. The Dockerfile should not `COPY` anything outside the `services/trading-dashboard/` directory — the build context is self-contained.

**Ports:**
- Development: 5173 (Vite dev server)
- Production: 3002 → nginx:80

## Visual Design

- Dark theme matching existing `live_ticker.html` aesthetic (deep blue/gray: `#1a1a2e`, `#16213e`)
- Monospace font for prices (SF Mono / Fira Code fallback)
- Color scheme: green (`#26a69a`) up, red (`#ef5350`) down, blue (`#2196f3`) MACD, orange (`#ff9800`) signal
- Responsive layout with CSS Grid
- Left sidebar: ~240px fixed width, collapsible on small screens

## Dependencies

```json
{
  "dependencies": {
    "react": "^18.3",
    "react-dom": "^18.3",
    "@apollo/client": "^3.11",
    "graphql": "^16.9",
    "graphql-ws": "^5.16",
    "lightweight-charts": "^4.2"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.3",
    "typescript": "^5.5",
    "vite": "^5.4"
  }
}
```

## Out of Scope (v1)

- Server-side watchlist persistence (mutations)
- Additional technical indicators beyond MACD
- Multiple chart timeframes (only daily candles)
- User authentication / API key management in frontend
- Mobile-specific responsive design
- Automated testing (deferred to implementation plan)
