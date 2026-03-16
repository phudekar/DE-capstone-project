# Trading Dashboard Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a React + Vite frontend that consumes the GraphQL API to display candlestick charts with MACD technical analysis and real-time trade streaming.

**Architecture:** Apollo Client with split HTTP/WS link fetches historical OHLCV data and streams live trades from the GraphQL API at port 8000. Lightweight Charts (TradingView) renders candlestick + volume + MACD. MACD computed client-side from close prices.

**Tech Stack:** React 18, Vite 5, TypeScript 5, Apollo Client 3, graphql-ws, Lightweight Charts 4, Vitest for testing.

**Spec:** `docs/superpowers/specs/2026-03-16-trading-dashboard-design.md`

---

## Chunk 1: Project Scaffolding + MACD Utils

### Task 1: Scaffold Vite + React + TypeScript project

**Files:**
- Create: `services/trading-dashboard/package.json`
- Create: `services/trading-dashboard/tsconfig.json`
- Create: `services/trading-dashboard/tsconfig.node.json`
- Create: `services/trading-dashboard/vite.config.ts`
- Create: `services/trading-dashboard/index.html`
- Create: `services/trading-dashboard/src/main.tsx`
- Create: `services/trading-dashboard/src/App.tsx`
- Create: `services/trading-dashboard/src/types/index.ts`
- Create: `services/trading-dashboard/.env`

- [ ] **Step 1: Create package.json with all dependencies**

```json
{
  "name": "trading-dashboard",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "preview": "vite preview",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "@apollo/client": "^3.11.0",
    "graphql": "^16.9.0",
    "graphql-ws": "^5.16.0",
    "lightweight-charts": "^4.2.0"
  },
  "devDependencies": {
    "@types/react": "^18.3.0",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "typescript": "^5.5.0",
    "vite": "^5.4.0",
    "vitest": "^2.1.0",
    "@testing-library/react": "^16.0.0",
    "@testing-library/jest-dom": "^6.5.0",
    "jsdom": "^25.0.0"
  }
}
```

- [ ] **Step 2: Create tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "forceConsistentCasingInFileNames": true
  },
  "include": ["src"]
}
```

- [ ] **Step 3: Create tsconfig.node.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "strict": true
  },
  "include": ["vite.config.ts"]
}
```

- [ ] **Step 4: Create vite.config.ts**

```typescript
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: [],
  },
});
```

- [ ] **Step 5: Create index.html**

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Trading Dashboard</title>
    <style>
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        background: #1a1a2e;
        color: #e0e0e0;
      }
    </style>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 6: Create .env**

```
VITE_GRAPHQL_HTTP_URL=http://localhost:8000/graphql
VITE_GRAPHQL_WS_URL=ws://localhost:8000/graphql
```

- [ ] **Step 7: Create TypeScript types**

Create `services/trading-dashboard/src/types/index.ts`:

```typescript
export interface OhlcvCandle {
  time: string; // ISO date string YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Trade {
  tradeId: string;
  symbol: string;
  price: number;
  quantity: number;
  timestamp: string;
  isAggressiveBuy: boolean;
}

export interface DailySummaryNode {
  symbol?: string;
  tradingDate: string;
  openPrice: number;
  closePrice: number;
  highPrice: number;
  lowPrice: number;
  totalVolume: number;
  tradeCount: number;
  totalValue: number;
  companyName: string | null;
  priceChange: number | null;
  priceChangePct: number | null;
}

export interface SymbolNode {
  symbol: string;
  companyName: string;
  sector: string;
  marketCapCategory: string;
  isCurrent: boolean;
}

export interface OrderBookSnapshot {
  symbol: string;
  bestBidPrice: number | null;
  bestBidQty: number | null;
  bestAskPrice: number | null;
  bestAskQty: number | null;
  bidDepth: number;
  askDepth: number;
  spread: number | null;
  midPrice: number | null;
}

export interface MarketOverview {
  tradingDate: string;
  totalTrades: number;
  uniqueSymbols: number;
  totalVolume: number;
  totalValue: number;
  advancing: number;
  declining: number;
  unchanged: number;
  topGainers: DailySummaryNode[];
  topLosers: DailySummaryNode[];
  mostActive: DailySummaryNode[];
}

export interface MacdPoint {
  time: string;
  macd: number;
  signal: number;
  histogram: number;
}

export interface PageInfo {
  hasNextPage: boolean;
  endCursor: string | null;
}

export interface Connection<T> {
  edges: Array<{ node: T; cursor: string }>;
  pageInfo: PageInfo;
  totalCount: number;
}
```

- [ ] **Step 8: Create minimal App.tsx**

```tsx
export default function App() {
  return (
    <div style={{ padding: "20px", color: "#e0e0e0" }}>
      <h1>Trading Dashboard</h1>
      <p>Loading...</p>
    </div>
  );
}
```

- [ ] **Step 9: Create main.tsx**

```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
```

- [ ] **Step 10: Install dependencies and verify build**

```bash
cd services/trading-dashboard && npm install && npm run build
```

Expected: Build succeeds, `dist/` directory created.

- [ ] **Step 11: Commit scaffold**

```bash
git add services/trading-dashboard/
git commit -m "feat(trading-dashboard): scaffold Vite + React + TypeScript project"
```

---

### Task 2: MACD calculation utility (TDD)

**Files:**
- Create: `services/trading-dashboard/src/utils/macd.ts`
- Create: `services/trading-dashboard/src/utils/macd.test.ts`

- [ ] **Step 1: Write failing tests for EMA and MACD**

Create `services/trading-dashboard/src/utils/macd.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { calculateEMA, calculateMACD } from "./macd";

describe("calculateEMA", () => {
  it("returns empty array for insufficient data", () => {
    expect(calculateEMA([1, 2], 3)).toEqual([]);
  });

  it("seeds with SMA for first value", () => {
    const prices = [10, 11, 12, 13, 14];
    const result = calculateEMA(prices, 3);
    // SMA of first 3 = (10+11+12)/3 = 11
    expect(result[0]).toBeCloseTo(11, 5);
  });

  it("calculates EMA correctly for period 3", () => {
    const prices = [10, 11, 12, 13, 14];
    const result = calculateEMA(prices, 3);
    // SMA seed = 11, multiplier = 2/(3+1) = 0.5
    // EMA[1] = (13 - 11) * 0.5 + 11 = 12
    // EMA[2] = (14 - 12) * 0.5 + 12 = 13
    expect(result).toHaveLength(3);
    expect(result[0]).toBeCloseTo(11, 5);
    expect(result[1]).toBeCloseTo(12, 5);
    expect(result[2]).toBeCloseTo(13, 5);
  });

  it("returns values aligned to end of input array", () => {
    const prices = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const result = calculateEMA(prices, 5);
    // Should have length = prices.length - period + 1 = 6
    expect(result).toHaveLength(6);
  });
});

describe("calculateMACD", () => {
  it("returns empty for insufficient data", () => {
    const prices = Array.from({ length: 20 }, (_, i) => 100 + i);
    const result = calculateMACD(prices);
    // Need at least 26 prices for EMA(26), result will be empty or very short
    expect(result.length).toBeLessThanOrEqual(0);
  });

  it("returns correct structure for sufficient data", () => {
    // 50 data points — enough for EMA(26) + signal(9)
    const prices = Array.from({ length: 50 }, (_, i) => 100 + Math.sin(i / 3) * 10);
    const result = calculateMACD(prices);

    expect(result.length).toBeGreaterThan(0);
    result.forEach((point) => {
      expect(point).toHaveProperty("macd");
      expect(point).toHaveProperty("signal");
      expect(point).toHaveProperty("histogram");
      expect(typeof point.macd).toBe("number");
      expect(typeof point.signal).toBe("number");
      expect(typeof point.histogram).toBe("number");
    });
  });

  it("histogram equals macd minus signal", () => {
    const prices = Array.from({ length: 60 }, (_, i) => 100 + Math.sin(i / 4) * 15);
    const result = calculateMACD(prices);

    result.forEach((point) => {
      expect(point.histogram).toBeCloseTo(point.macd - point.signal, 10);
    });
  });

  it("uses standard 12/26/9 parameters by default", () => {
    const prices = Array.from({ length: 60 }, (_, i) => 100 + i * 0.5);
    const result12_26_9 = calculateMACD(prices);
    const resultCustom = calculateMACD(prices, 12, 26, 9);
    expect(result12_26_9).toEqual(resultCustom);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd services/trading-dashboard && npx vitest run src/utils/macd.test.ts
```

Expected: FAIL — `macd.ts` does not exist yet.

- [ ] **Step 3: Implement MACD calculation**

Create `services/trading-dashboard/src/utils/macd.ts`:

```typescript
export interface MacdResult {
  macd: number;
  signal: number;
  histogram: number;
}

/**
 * Calculate Exponential Moving Average.
 * Returns array of length (prices.length - period + 1).
 * First value is seeded with SMA of the first `period` prices.
 */
export function calculateEMA(prices: number[], period: number): number[] {
  if (prices.length < period) return [];

  const multiplier = 2 / (period + 1);
  const result: number[] = [];

  // Seed with SMA
  let sum = 0;
  for (let i = 0; i < period; i++) {
    sum += prices[i];
  }
  result.push(sum / period);

  // Calculate EMA for remaining prices
  for (let i = period; i < prices.length; i++) {
    const ema = (prices[i] - result[result.length - 1]) * multiplier + result[result.length - 1];
    result.push(ema);
  }

  return result;
}

/**
 * Calculate MACD (Moving Average Convergence Divergence).
 * Default parameters: fast=12, slow=26, signal=9 (industry standard).
 * Returns array of { macd, signal, histogram } values.
 */
export function calculateMACD(
  closePrices: number[],
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9,
): MacdResult[] {
  const fastEMA = calculateEMA(closePrices, fastPeriod);
  const slowEMA = calculateEMA(closePrices, slowPeriod);

  if (slowEMA.length === 0) return [];

  // Align fast and slow EMAs (slow is shorter)
  const offset = fastEMA.length - slowEMA.length;
  const macdLine: number[] = [];
  for (let i = 0; i < slowEMA.length; i++) {
    macdLine.push(fastEMA[i + offset] - slowEMA[i]);
  }

  // Signal line = EMA of MACD line
  const signalLine = calculateEMA(macdLine, signalPeriod);
  if (signalLine.length === 0) return [];

  // Align MACD and signal
  const signalOffset = macdLine.length - signalLine.length;
  const results: MacdResult[] = [];
  for (let i = 0; i < signalLine.length; i++) {
    const macd = macdLine[i + signalOffset];
    const signal = signalLine[i];
    results.push({
      macd,
      signal,
      histogram: macd - signal,
    });
  }

  return results;
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd services/trading-dashboard && npx vitest run src/utils/macd.test.ts
```

Expected: All tests PASS.

- [ ] **Step 5: Commit MACD utility**

```bash
git add services/trading-dashboard/src/utils/macd.ts services/trading-dashboard/src/utils/macd.test.ts
git commit -m "feat(trading-dashboard): add MACD calculation utility with tests"
```

---

### Task 3: Candle aggregation utility (TDD)

**Files:**
- Create: `services/trading-dashboard/src/utils/candle.ts`
- Create: `services/trading-dashboard/src/utils/candle.test.ts`

- [ ] **Step 1: Write failing tests for candle aggregation**

Create `services/trading-dashboard/src/utils/candle.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { createCandleFromTrade, updateCandleWithTrade } from "./candle";
import type { OhlcvCandle, Trade } from "../types";

const makeTrade = (overrides: Partial<Trade> = {}): Trade => ({
  tradeId: "t1",
  symbol: "AAPL",
  price: 150.0,
  quantity: 100,
  timestamp: "2026-03-16T10:30:00Z",
  isAggressiveBuy: true,
  ...overrides,
});

describe("createCandleFromTrade", () => {
  it("creates a candle with OHLC all set to trade price", () => {
    const trade = makeTrade({ price: 150.0, quantity: 100 });
    const candle = createCandleFromTrade(trade);
    expect(candle.open).toBe(150.0);
    expect(candle.high).toBe(150.0);
    expect(candle.low).toBe(150.0);
    expect(candle.close).toBe(150.0);
    expect(candle.volume).toBe(100);
    expect(candle.time).toBe("2026-03-16");
  });
});

describe("updateCandleWithTrade", () => {
  const baseCandle: OhlcvCandle = {
    time: "2026-03-16",
    open: 150.0,
    high: 155.0,
    low: 148.0,
    close: 152.0,
    volume: 1000,
  };

  it("updates close to latest trade price", () => {
    const trade = makeTrade({ price: 153.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.close).toBe(153.0);
  });

  it("updates high when trade price exceeds current high", () => {
    const trade = makeTrade({ price: 160.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.high).toBe(160.0);
  });

  it("updates low when trade price is below current low", () => {
    const trade = makeTrade({ price: 145.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.low).toBe(145.0);
  });

  it("accumulates volume", () => {
    const trade = makeTrade({ quantity: 200 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.volume).toBe(1200);
  });

  it("does not change open", () => {
    const trade = makeTrade({ price: 999.0 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.open).toBe(150.0);
  });

  it("preserves time", () => {
    const trade = makeTrade();
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.time).toBe("2026-03-16");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd services/trading-dashboard && npx vitest run src/utils/candle.test.ts
```

Expected: FAIL — `candle.ts` does not exist yet.

- [ ] **Step 3: Implement candle aggregation**

Create `services/trading-dashboard/src/utils/candle.ts`:

```typescript
import type { OhlcvCandle, Trade } from "../types";

/**
 * Extract the date portion (YYYY-MM-DD) from an ISO timestamp.
 */
function toDateString(timestamp: string): string {
  return timestamp.slice(0, 10);
}

/**
 * Create a new candle from a single trade.
 * Used when the first trade of a new day arrives.
 */
export function createCandleFromTrade(trade: Trade): OhlcvCandle {
  return {
    time: toDateString(trade.timestamp),
    open: trade.price,
    high: trade.price,
    low: trade.price,
    close: trade.price,
    volume: trade.quantity,
  };
}

/**
 * Update an existing candle with a new trade.
 * Updates close, high/low if exceeded, and accumulates volume.
 * Open is never changed.
 */
export function updateCandleWithTrade(candle: OhlcvCandle, trade: Trade): OhlcvCandle {
  return {
    time: candle.time,
    open: candle.open,
    high: Math.max(candle.high, trade.price),
    low: Math.min(candle.low, trade.price),
    close: trade.price,
    volume: candle.volume + trade.quantity,
  };
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd services/trading-dashboard && npx vitest run src/utils/candle.test.ts
```

Expected: All tests PASS.

- [ ] **Step 5: Commit candle utility**

```bash
git add services/trading-dashboard/src/utils/candle.ts services/trading-dashboard/src/utils/candle.test.ts
git commit -m "feat(trading-dashboard): add candle aggregation utility with tests"
```

---

## Chunk 2: GraphQL Client + Hooks

### Task 4: Apollo Client setup (split HTTP/WS link)

**Files:**
- Create: `services/trading-dashboard/src/graphql/client.ts`
- Create: `services/trading-dashboard/src/graphql/queries.ts`
- Create: `services/trading-dashboard/src/graphql/subscriptions.ts`

- [ ] **Step 1: Create Apollo Client with split link**

Create `services/trading-dashboard/src/graphql/client.ts`:

```typescript
import { ApolloClient, InMemoryCache, HttpLink, split } from "@apollo/client";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions";
import { getMainDefinition } from "@apollo/client/utilities";
import { createClient } from "graphql-ws";

const httpUrl = import.meta.env.VITE_GRAPHQL_HTTP_URL || "http://localhost:8000/graphql";
const wsUrl = import.meta.env.VITE_GRAPHQL_WS_URL || "ws://localhost:8000/graphql";

const httpLink = new HttpLink({ uri: httpUrl });

const wsLink = new GraphQLWsLink(
  createClient({
    url: wsUrl,
    retryAttempts: Infinity,
    shouldRetry: () => true,
    connectionParams: {},
  }),
);

const splitLink = split(
  ({ query }) => {
    const definition = getMainDefinition(query);
    return definition.kind === "OperationDefinition" && definition.operation === "subscription";
  },
  wsLink,
  httpLink,
);

export const apolloClient = new ApolloClient({
  link: splitLink,
  cache: new InMemoryCache(),
});
```

- [ ] **Step 2: Create GraphQL query documents**

Create `services/trading-dashboard/src/graphql/queries.ts`:

```typescript
import { gql } from "@apollo/client";

export const DAILY_SUMMARY_QUERY = gql`
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
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
    }
  }
`;

export const SYMBOLS_QUERY = gql`
  query Symbols($first: Int, $after: String) {
    symbols(first: $first, after: $after) {
      edges {
        node {
          symbol
          companyName
          sector
          marketCapCategory
          isCurrent
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
    }
  }
`;

export const MARKET_OVERVIEW_QUERY = gql`
  query MarketOverview($targetDate: Date) {
    marketOverview(targetDate: $targetDate) {
      tradingDate
      totalTrades
      uniqueSymbols
      totalVolume
      totalValue
      advancing
      declining
      unchanged
      topGainers {
        symbol
        tradingDate
        openPrice
        closePrice
        highPrice
        lowPrice
        totalVolume
        companyName
        priceChange
        priceChangePct
      }
      topLosers {
        symbol
        tradingDate
        openPrice
        closePrice
        highPrice
        lowPrice
        totalVolume
        companyName
        priceChange
        priceChangePct
      }
      mostActive {
        symbol
        tradingDate
        openPrice
        closePrice
        totalVolume
        companyName
        priceChangePct
      }
    }
  }
`;

export const ORDER_BOOK_QUERY = gql`
  query OrderBook($symbol: String!) {
    orderBook(symbol: $symbol) {
      symbol
      bestBidPrice
      bestBidQty
      bestAskPrice
      bestAskQty
      bidDepth
      askDepth
      spread
      midPrice
    }
  }
`;

export const WATCHLIST_PRICE_QUERY = gql`
  query WatchlistPrice($symbol: String!, $dateRange: DateRangeInput!) {
    dailySummary(symbol: $symbol, dateRange: $dateRange, first: 1) {
      edges {
        node {
          closePrice
          priceChangePct
        }
      }
    }
  }
`;
```

- [ ] **Step 3: Create subscription document**

Create `services/trading-dashboard/src/graphql/subscriptions.ts`:

```typescript
import { gql } from "@apollo/client";

export const ON_NEW_TRADE_SUBSCRIPTION = gql`
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
`;
```

- [ ] **Step 4: Verify build still passes**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 5: Commit GraphQL layer**

```bash
git add services/trading-dashboard/src/graphql/
git commit -m "feat(trading-dashboard): add Apollo Client setup with queries and subscriptions"
```

---

### Task 5: useOhlcvData hook (TDD)

**Files:**
- Create: `services/trading-dashboard/src/hooks/useOhlcvData.ts`
- Create: `services/trading-dashboard/src/hooks/useOhlcvData.test.ts`

- [ ] **Step 1: Write failing test for OHLCV data hook**

Create `services/trading-dashboard/src/hooks/useOhlcvData.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { transformDailySummaryToCandles } from "./useOhlcvData";
import type { DailySummaryNode } from "../types";

describe("transformDailySummaryToCandles", () => {
  it("converts DailySummaryNode array to OhlcvCandle array", () => {
    const nodes: DailySummaryNode[] = [
      {
        tradingDate: "2026-03-14",
        openPrice: 150.0,
        closePrice: 152.0,
        highPrice: 155.0,
        lowPrice: 149.0,
        totalVolume: 50000,
        tradeCount: 1200,
        totalValue: 7500000,
        companyName: "Apple Inc.",
        priceChange: 2.0,
        priceChangePct: 1.33,
      },
      {
        tradingDate: "2026-03-15",
        openPrice: 152.0,
        closePrice: 148.0,
        highPrice: 153.0,
        lowPrice: 147.0,
        totalVolume: 60000,
        tradeCount: 1500,
        totalValue: 8900000,
        companyName: "Apple Inc.",
        priceChange: -4.0,
        priceChangePct: -2.63,
      },
    ];

    const candles = transformDailySummaryToCandles(nodes);

    expect(candles).toHaveLength(2);
    expect(candles[0]).toEqual({
      time: "2026-03-14",
      open: 150.0,
      high: 155.0,
      low: 149.0,
      close: 152.0,
      volume: 50000,
    });
    expect(candles[1]).toEqual({
      time: "2026-03-15",
      open: 152.0,
      high: 153.0,
      low: 147.0,
      close: 148.0,
      volume: 60000,
    });
  });

  it("returns empty array for empty input", () => {
    expect(transformDailySummaryToCandles([])).toEqual([]);
  });

  it("sorts candles by date ascending", () => {
    const nodes: DailySummaryNode[] = [
      {
        tradingDate: "2026-03-16",
        openPrice: 100, closePrice: 101, highPrice: 102, lowPrice: 99,
        totalVolume: 1000, tradeCount: 10, totalValue: 100000,
        companyName: null, priceChange: null, priceChangePct: null,
      },
      {
        tradingDate: "2026-03-14",
        openPrice: 98, closePrice: 99, highPrice: 100, lowPrice: 97,
        totalVolume: 800, tradeCount: 8, totalValue: 80000,
        companyName: null, priceChange: null, priceChangePct: null,
      },
    ];

    const candles = transformDailySummaryToCandles(nodes);
    expect(candles[0].time).toBe("2026-03-14");
    expect(candles[1].time).toBe("2026-03-16");
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd services/trading-dashboard && npx vitest run src/hooks/useOhlcvData.test.ts
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement useOhlcvData hook**

Create `services/trading-dashboard/src/hooks/useOhlcvData.ts`:

```typescript
import { useQuery } from "@apollo/client";
import { useMemo } from "react";
import { DAILY_SUMMARY_QUERY } from "../graphql/queries";
import type { OhlcvCandle, DailySummaryNode, Connection } from "../types";

/**
 * Transform API DailySummaryNode[] into chart-ready OhlcvCandle[].
 * Exported separately for testability.
 */
export function transformDailySummaryToCandles(nodes: DailySummaryNode[]): OhlcvCandle[] {
  return nodes
    .map((node) => ({
      time: node.tradingDate,
      open: node.openPrice,
      high: node.highPrice,
      low: node.lowPrice,
      close: node.closePrice,
      volume: node.totalVolume,
    }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

interface DailySummaryData {
  dailySummary: Connection<DailySummaryNode>;
}

/**
 * Fetch historical OHLCV data for a symbol.
 * Requests up to 365 candles (1 year of daily data).
 */
export function useOhlcvData(symbol: string) {
  const today = new Date().toISOString().slice(0, 10);
  const oneYearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const { data, loading, error, fetchMore } = useQuery<DailySummaryData>(DAILY_SUMMARY_QUERY, {
    variables: {
      symbol,
      dateRange: { start: oneYearAgo, end: today },
      first: 365,
    },
    skip: !symbol,
  });

  const candles = useMemo(() => {
    if (!data?.dailySummary?.edges) return [];
    const nodes = data.dailySummary.edges.map((edge) => edge.node);
    return transformDailySummaryToCandles(nodes);
  }, [data]);

  const latestSummary = useMemo(() => {
    if (!data?.dailySummary?.edges?.length) return null;
    const nodes = data.dailySummary.edges.map((e) => e.node);
    return nodes[nodes.length - 1];
  }, [data]);

  return { candles, loading, error, latestSummary, fetchMore };
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd services/trading-dashboard && npx vitest run src/hooks/useOhlcvData.test.ts
```

Expected: All tests PASS.

- [ ] **Step 5: Commit hook**

```bash
git add services/trading-dashboard/src/hooks/useOhlcvData.ts services/trading-dashboard/src/hooks/useOhlcvData.test.ts
git commit -m "feat(trading-dashboard): add useOhlcvData hook with data transformation"
```

---

### Task 6: useMacd hook

**Files:**
- Create: `services/trading-dashboard/src/hooks/useMacd.ts`
- Create: `services/trading-dashboard/src/hooks/useMacd.test.ts`

- [ ] **Step 1: Write failing test**

Create `services/trading-dashboard/src/hooks/useMacd.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { computeMacdFromCandles } from "./useMacd";
import type { OhlcvCandle } from "../types";

function makeCandles(count: number): OhlcvCandle[] {
  return Array.from({ length: count }, (_, i) => ({
    time: `2026-01-${String(i + 1).padStart(2, "0")}`,
    open: 100 + Math.sin(i / 3) * 10,
    high: 105 + Math.sin(i / 3) * 10,
    low: 95 + Math.sin(i / 3) * 10,
    close: 100 + Math.sin(i / 3) * 10 + (i % 2 === 0 ? 1 : -1),
    volume: 10000 + i * 100,
  }));
}

describe("computeMacdFromCandles", () => {
  it("returns empty array for fewer than 26 candles", () => {
    const candles = makeCandles(20);
    expect(computeMacdFromCandles(candles)).toEqual([]);
  });

  it("returns MACD points with time alignment for sufficient data", () => {
    const candles = makeCandles(60);
    const result = computeMacdFromCandles(candles);

    expect(result.length).toBeGreaterThan(0);
    result.forEach((point) => {
      expect(point.time).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(typeof point.macd).toBe("number");
      expect(typeof point.signal).toBe("number");
      expect(typeof point.histogram).toBe("number");
    });
  });

  it("aligns MACD points to the end of candle dates", () => {
    const candles = makeCandles(60);
    const result = computeMacdFromCandles(candles);

    // Last MACD point time should equal last candle time
    expect(result[result.length - 1].time).toBe(candles[candles.length - 1].time);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd services/trading-dashboard && npx vitest run src/hooks/useMacd.test.ts
```

Expected: FAIL.

- [ ] **Step 3: Implement useMacd hook**

Create `services/trading-dashboard/src/hooks/useMacd.ts`:

```typescript
import { useMemo } from "react";
import { calculateMACD } from "../utils/macd";
import type { OhlcvCandle, MacdPoint } from "../types";

/**
 * Compute MACD points from candles, aligning each result to a candle date.
 * Exported for testability.
 */
export function computeMacdFromCandles(candles: OhlcvCandle[]): MacdPoint[] {
  if (candles.length < 26) return [];

  const closePrices = candles.map((c) => c.close);
  const macdResults = calculateMACD(closePrices);

  if (macdResults.length === 0) return [];

  // MACD results align to the end of the candle array
  const offset = candles.length - macdResults.length;
  return macdResults.map((result, i) => ({
    time: candles[i + offset].time,
    macd: result.macd,
    signal: result.signal,
    histogram: result.histogram,
  }));
}

/**
 * React hook that computes MACD from OHLCV candles.
 * Recomputes when candles change.
 */
export function useMacd(candles: OhlcvCandle[]): MacdPoint[] {
  return useMemo(() => computeMacdFromCandles(candles), [candles]);
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd services/trading-dashboard && npx vitest run src/hooks/useMacd.test.ts
```

Expected: All tests PASS.

- [ ] **Step 5: Commit hook**

```bash
git add services/trading-dashboard/src/hooks/useMacd.ts services/trading-dashboard/src/hooks/useMacd.test.ts
git commit -m "feat(trading-dashboard): add useMacd hook with date-aligned MACD computation"
```

---

### Task 7: useTradeSubscription hook

**Files:**
- Create: `services/trading-dashboard/src/hooks/useTradeSubscription.ts`

- [ ] **Step 1: Implement trade subscription hook**

Create `services/trading-dashboard/src/hooks/useTradeSubscription.ts`:

```typescript
import { useSubscription } from "@apollo/client";
import { useCallback, useRef, useState } from "react";
import { ON_NEW_TRADE_SUBSCRIPTION } from "../graphql/subscriptions";
import { createCandleFromTrade, updateCandleWithTrade } from "../utils/candle";
import type { OhlcvCandle, Trade } from "../types";
import type { ConnectionState } from "../components/ConnectionStatus";

interface SubscriptionData {
  onNewTrade: Trade;
}

interface TradeSubscriptionResult {
  liveCandle: OhlcvCandle | null;
  lastTrade: Trade | null;
  connectionStatus: ConnectionState;
  tradeCount: number;
}

/**
 * Subscribe to real-time trades for a symbol.
 * Aggregates incoming trades into a live-building candle.
 * Returns the current live candle, latest trade, and connection status.
 */
export function useTradeSubscription(symbol: string): TradeSubscriptionResult {
  const [liveCandle, setLiveCandle] = useState<OhlcvCandle | null>(null);
  const [lastTrade, setLastTrade] = useState<Trade | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionState>("connecting");
  const tradeCountRef = useRef(0);
  const [tradeCount, setTradeCount] = useState(0);

  const onData = useCallback(
    ({ data }: { data?: { data?: SubscriptionData } }) => {
      const trade = data?.data?.onNewTrade;
      if (!trade) return;

      setConnectionStatus("live");
      setLastTrade(trade);
      tradeCountRef.current += 1;
      setTradeCount(tradeCountRef.current);

      setLiveCandle((prev) => {
        if (!prev) return createCandleFromTrade(trade);

        const tradeDate = trade.timestamp.slice(0, 10);
        if (tradeDate !== prev.time) {
          // New day — start a fresh candle
          return createCandleFromTrade(trade);
        }

        return updateCandleWithTrade(prev, trade);
      });
    },
    [],
  );

  useSubscription<SubscriptionData>(ON_NEW_TRADE_SUBSCRIPTION, {
    variables: { symbol },
    skip: !symbol,
    onData,
    onError: () => setConnectionStatus("disconnected"),
  });

  return { liveCandle, lastTrade, connectionStatus, tradeCount };
}
```

- [ ] **Step 2: Verify build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 3: Commit hook**

```bash
git add services/trading-dashboard/src/hooks/useTradeSubscription.ts
git commit -m "feat(trading-dashboard): add useTradeSubscription hook for live candle aggregation"
```

---

## Chunk 3: UI Components

### Task 8: CandlestickChart component

**Files:**
- Create: `services/trading-dashboard/src/components/CandlestickChart.tsx`

- [ ] **Step 1: Implement candlestick chart**

Create `services/trading-dashboard/src/components/CandlestickChart.tsx`:

```tsx
import { useEffect, useRef } from "react";
import {
  createChart,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type Time,
  ColorType,
  CrosshairMode,
} from "lightweight-charts";
import type { OhlcvCandle } from "../types";

interface Props {
  candles: OhlcvCandle[];
  liveCandle: OhlcvCandle | null;
  onVisibleRangeChange?: (range: { from: number; to: number } | null) => void;
}

function toCandlestickData(candle: OhlcvCandle): CandlestickData<Time> {
  return {
    time: candle.time as Time,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

function toVolumeData(candle: OhlcvCandle): HistogramData<Time> {
  return {
    time: candle.time as Time,
    value: candle.volume,
    color: candle.close >= candle.open ? "rgba(38, 166, 154, 0.4)" : "rgba(239, 83, 80, 0.4)",
  };
}

export default function CandlestickChart({ candles, liveCandle, onVisibleRangeChange }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Create chart on mount
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#1a1a2e" },
        textColor: "#888",
      },
      grid: {
        vertLines: { color: "#2a2a4a" },
        horzLines: { color: "#2a2a4a" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#2a2a4a" },
      timeScale: { borderColor: "#2a2a4a", timeVisible: false },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderDownColor: "#ef5350",
      borderUpColor: "#26a69a",
      wickDownColor: "#ef5350",
      wickUpColor: "#26a69a",
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });

    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    if (onVisibleRangeChange) {
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        onVisibleRangeChange(range ? { from: range.from, to: range.to } : null);
      });
    }

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    const resizeObserver = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      chart.applyOptions({ width, height });
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, []);

  // Update historical data
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || candles.length === 0) return;
    candleSeriesRef.current.setData(candles.map(toCandlestickData));
    volumeSeriesRef.current.setData(candles.map(toVolumeData));
    chartRef.current?.timeScale().fitContent();
  }, [candles]);

  // Update live candle
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || !liveCandle) return;
    candleSeriesRef.current.update(toCandlestickData(liveCandle));
    volumeSeriesRef.current.update(toVolumeData(liveCandle));
  }, [liveCandle]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", minHeight: "300px" }}
    />
  );
}
```

- [ ] **Step 2: Verify build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 3: Commit component**

```bash
git add services/trading-dashboard/src/components/CandlestickChart.tsx
git commit -m "feat(trading-dashboard): add CandlestickChart component with live update"
```

---

### Task 9: MacdChart component

**Files:**
- Create: `services/trading-dashboard/src/components/MacdChart.tsx`

- [ ] **Step 1: Implement MACD chart**

Create `services/trading-dashboard/src/components/MacdChart.tsx`:

```tsx
import { useEffect, useRef } from "react";
import {
  createChart,
  type IChartApi,
  type ISeriesApi,
  type HistogramData,
  type LineData,
  type Time,
  ColorType,
  CrosshairMode,
} from "lightweight-charts";
import type { MacdPoint } from "../types";

interface Props {
  macdData: MacdPoint[];
  visibleRange?: { from: number; to: number } | null;
}

export default function MacdChart({ macdData, visibleRange }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const macdLineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const signalLineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const histogramRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Create chart on mount
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#1a1a2e" },
        textColor: "#888",
      },
      grid: {
        vertLines: { color: "#2a2a4a" },
        horzLines: { color: "#2a2a4a" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#2a2a4a" },
      timeScale: { borderColor: "#2a2a4a", timeVisible: false },
    });

    const histogram = chart.addHistogramSeries({
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    const macdLine = chart.addLineSeries({
      color: "#2196f3",
      lineWidth: 2,
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    const signalLine = chart.addLineSeries({
      color: "#ff9800",
      lineWidth: 2,
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    chartRef.current = chart;
    macdLineRef.current = macdLine;
    signalLineRef.current = signalLine;
    histogramRef.current = histogram;

    const resizeObserver = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      chart.applyOptions({ width, height });
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, []);

  // Update MACD data
  useEffect(() => {
    if (!macdLineRef.current || !signalLineRef.current || !histogramRef.current) return;
    if (macdData.length === 0) return;

    const macdLineData: LineData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.macd,
    }));

    const signalLineData: LineData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.signal,
    }));

    const histogramData: HistogramData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.histogram,
      color: p.histogram >= 0 ? "#26a69a" : "#ef5350",
    }));

    histogramRef.current.setData(histogramData);
    macdLineRef.current.setData(macdLineData);
    signalLineRef.current.setData(signalLineData);
  }, [macdData]);

  // Sync visible range from main chart
  useEffect(() => {
    if (!chartRef.current || !visibleRange) return;
    chartRef.current.timeScale().setVisibleLogicalRange({
      from: visibleRange.from,
      to: visibleRange.to,
    });
  }, [visibleRange]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", minHeight: "120px" }}
    />
  );
}
```

- [ ] **Step 2: Verify build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 3: Commit component**

```bash
git add services/trading-dashboard/src/components/MacdChart.tsx
git commit -m "feat(trading-dashboard): add MacdChart component with synced time scale"
```

---

### Task 10: Sidebar components (Watchlist, OrderBook, SymbolHeader, ConnectionStatus)

**Files:**
- Create: `services/trading-dashboard/src/components/SymbolHeader.tsx`
- Create: `services/trading-dashboard/src/components/Watchlist.tsx`
- Create: `services/trading-dashboard/src/components/OrderBook.tsx`
- Create: `services/trading-dashboard/src/components/ConnectionStatus.tsx`

- [ ] **Step 1: Implement SymbolHeader**

Create `services/trading-dashboard/src/components/SymbolHeader.tsx`:

```tsx
import type { DailySummaryNode, Trade } from "../types";

interface Props {
  symbol: string;
  lastTrade: Trade | null;
  dailySummary: DailySummaryNode | null;
  tradeCount: number;
}

export default function SymbolHeader({ symbol, lastTrade, dailySummary, tradeCount }: Props) {
  const price = lastTrade?.price ?? dailySummary?.closePrice ?? 0;
  const change = dailySummary?.priceChange ?? 0;
  const changePct = dailySummary?.priceChangePct ?? 0;
  const isPositive = change >= 0;
  const color = isPositive ? "#26a69a" : "#ef5350";
  const arrow = isPositive ? "\u25B2" : "\u25BC";

  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      gap: "16px",
      padding: "12px 16px",
      background: "#16213e",
      borderRadius: "6px",
    }}>
      <span style={{ fontSize: "18px", fontWeight: "bold", color: "#fff" }}>{symbol}</span>
      {dailySummary?.companyName && (
        <span style={{ fontSize: "12px", color: "#888" }}>{dailySummary.companyName}</span>
      )}
      <span style={{
        fontFamily: "'SF Mono', 'Fira Code', monospace",
        fontSize: "18px",
        fontWeight: "bold",
        color,
      }}>
        ${price.toFixed(2)}
      </span>
      <span style={{ color, fontSize: "14px" }}>
        {arrow} {change >= 0 ? "+" : ""}{change.toFixed(2)} ({changePct >= 0 ? "+" : ""}{changePct.toFixed(2)}%)
      </span>
      <div style={{ marginLeft: "auto", fontSize: "12px", color: "#888" }}>
        <span>H: ${dailySummary?.highPrice?.toFixed(2) ?? "—"}</span>
        <span style={{ marginLeft: "12px" }}>L: ${dailySummary?.lowPrice?.toFixed(2) ?? "—"}</span>
        <span style={{ marginLeft: "12px" }}>V: {dailySummary?.totalVolume?.toLocaleString() ?? "—"}</span>
        <span style={{ marginLeft: "12px" }}>Trades: {tradeCount}</span>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Implement Watchlist**

Create `services/trading-dashboard/src/components/Watchlist.tsx`:

```tsx
interface WatchlistItem {
  symbol: string;
  price: number | null;
  changePct: number | null;
}

interface Props {
  items: WatchlistItem[];
  activeSymbol: string;
  onSelectSymbol: (symbol: string) => void;
}

export default function Watchlist({ items, activeSymbol, onSelectSymbol }: Props) {
  return (
    <div style={{
      background: "#16213e",
      borderRadius: "6px",
      padding: "12px",
      flex: 1,
      overflowY: "auto",
    }}>
      <div style={{
        fontSize: "11px",
        fontWeight: "bold",
        color: "#26a69a",
        textTransform: "uppercase",
        marginBottom: "8px",
        letterSpacing: "1px",
      }}>
        Watchlist
      </div>
      {items.map((item) => {
        const isActive = item.symbol === activeSymbol;
        const color = (item.changePct ?? 0) >= 0 ? "#26a69a" : "#ef5350";
        return (
          <div
            key={item.symbol}
            onClick={() => onSelectSymbol(item.symbol)}
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "6px 8px",
              cursor: "pointer",
              borderRadius: "4px",
              borderLeft: isActive ? "3px solid #2196f3" : "3px solid transparent",
              background: isActive ? "rgba(33, 150, 243, 0.1)" : "transparent",
              fontSize: "13px",
              fontFamily: "'SF Mono', 'Fira Code', monospace",
            }}
          >
            <span style={{ color: "#fff" }}>{item.symbol}</span>
            <div style={{ textAlign: "right" }}>
              {item.price != null && (
                <div style={{ color: "#ccc", fontSize: "12px" }}>
                  ${item.price.toFixed(2)}
                </div>
              )}
              {item.changePct != null && (
                <div style={{ color, fontSize: "11px" }}>
                  {item.changePct >= 0 ? "+" : ""}{item.changePct.toFixed(2)}%
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
```

- [ ] **Step 3: Implement OrderBook**

Create `services/trading-dashboard/src/components/OrderBook.tsx`:

```tsx
import { useQuery } from "@apollo/client";
import { ORDER_BOOK_QUERY } from "../graphql/queries";
import type { OrderBookSnapshot } from "../types";

interface Props {
  symbol: string;
}

export default function OrderBook({ symbol }: Props) {
  const { data } = useQuery<{ orderBook: OrderBookSnapshot | null }>(ORDER_BOOK_QUERY, {
    variables: { symbol },
    pollInterval: 5000,
    skip: !symbol,
  });

  const book = data?.orderBook;

  return (
    <div style={{
      background: "#16213e",
      borderRadius: "6px",
      padding: "12px",
    }}>
      <div style={{
        fontSize: "11px",
        fontWeight: "bold",
        color: "#2196f3",
        textTransform: "uppercase",
        marginBottom: "8px",
        letterSpacing: "1px",
      }}>
        Order Book
      </div>
      {!book ? (
        <div style={{ color: "#555", fontSize: "12px" }}>No data</div>
      ) : (
        <div style={{ fontFamily: "'SF Mono', 'Fira Code', monospace", fontSize: "12px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", color: "#26a69a", padding: "3px 0" }}>
            <span>BID</span>
            <span>{book.bestBidPrice?.toFixed(2) ?? "—"}</span>
            <span>{book.bestBidQty ?? "—"}</span>
          </div>
          <div style={{
            textAlign: "center",
            padding: "6px 0",
            margin: "4px 0",
            border: "1px solid #444",
            borderRadius: "3px",
            color: "#fff",
            fontSize: "11px",
          }}>
            Spread: {book.spread?.toFixed(2) ?? "—"} | Mid: {book.midPrice?.toFixed(2) ?? "—"}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", color: "#ef5350", padding: "3px 0" }}>
            <span>ASK</span>
            <span>{book.bestAskPrice?.toFixed(2) ?? "—"}</span>
            <span>{book.bestAskQty ?? "—"}</span>
          </div>
          <div style={{ marginTop: "8px", fontSize: "10px", color: "#555" }}>
            Bid depth: {book.bidDepth} | Ask depth: {book.askDepth}
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 4: Implement ConnectionStatus**

Create `services/trading-dashboard/src/components/ConnectionStatus.tsx`:

```tsx
export type ConnectionState = "live" | "connecting" | "disconnected";

interface Props {
  status: ConnectionState;
}

const STATUS_CONFIG: Record<ConnectionState, { label: string; color: string }> = {
  live: { label: "LIVE", color: "#26a69a" },
  connecting: { label: "CONNECTING", color: "#ff9800" },
  disconnected: { label: "DISCONNECTED", color: "#ef5350" },
};

export default function ConnectionStatus({ status }: Props) {
  const { label, color } = STATUS_CONFIG[status];

  return (
    <div style={{
      display: "inline-flex",
      alignItems: "center",
      gap: "6px",
      padding: "4px 10px",
      borderRadius: "12px",
      background: `${color}22`,
      fontSize: "11px",
      fontWeight: "bold",
      color,
      letterSpacing: "1px",
    }}>
      <span style={{
        width: "8px",
        height: "8px",
        borderRadius: "50%",
        background: color,
        animation: status === "live" ? "pulse 2s infinite" : "none",
      }} />
      {label}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}
```

- [ ] **Step 5: Verify build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 6: Commit all sidebar components**

```bash
git add services/trading-dashboard/src/components/
git commit -m "feat(trading-dashboard): add SymbolHeader, Watchlist, OrderBook, ConnectionStatus components"
```

---

## Chunk 4: App Assembly + Docker + Integration

### Task 11: Wire up App.tsx with all components

**Files:**
- Modify: `services/trading-dashboard/src/App.tsx`
- Modify: `services/trading-dashboard/src/main.tsx`

- [ ] **Step 1: Update main.tsx with ApolloProvider**

Update `services/trading-dashboard/src/main.tsx`:

```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ApolloProvider } from "@apollo/client";
import { apolloClient } from "./graphql/client";
import App from "./App";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ApolloProvider client={apolloClient}>
      <App />
    </ApolloProvider>
  </StrictMode>
);
```

- [ ] **Step 2: Implement full App.tsx layout**

Update `services/trading-dashboard/src/App.tsx`:

```tsx
import { useState, useCallback } from "react";
import { useQuery } from "@apollo/client";
import CandlestickChart from "./components/CandlestickChart";
import MacdChart from "./components/MacdChart";
import Watchlist from "./components/Watchlist";
import OrderBook from "./components/OrderBook";
import SymbolHeader from "./components/SymbolHeader";
import ConnectionStatus from "./components/ConnectionStatus";
import { useOhlcvData } from "./hooks/useOhlcvData";
import { useMacd } from "./hooks/useMacd";
import { useTradeSubscription } from "./hooks/useTradeSubscription";
import { SYMBOLS_QUERY, MARKET_OVERVIEW_QUERY } from "./graphql/queries";
import type { SymbolNode, MarketOverview, Connection } from "./types";

const DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "JPM"];

export default function App() {
  const [activeSymbol, setActiveSymbol] = useState("AAPL");
  const [visibleRange, setVisibleRange] = useState<{ from: number; to: number } | null>(null);

  // Historical data
  const { candles, loading, latestSummary } = useOhlcvData(activeSymbol);

  // MACD from historical candles
  const macdData = useMacd(candles);

  // Real-time trades
  const { liveCandle, lastTrade, connectionStatus, tradeCount } = useTradeSubscription(activeSymbol);

  // Symbols for watchlist
  const { data: symbolsData } = useQuery<{ symbols: Connection<SymbolNode> }>(SYMBOLS_QUERY, {
    variables: { first: 50 },
  });

  // Market overview for watchlist prices
  const { data: overviewData } = useQuery<{ marketOverview: MarketOverview }>(MARKET_OVERVIEW_QUERY, {
    pollInterval: 60000,
  });

  const handleRangeChange = useCallback((range: { from: number; to: number } | null) => {
    setVisibleRange(range);
  }, []);

  // Build watchlist items — match by symbol field on DailySummary
  const watchlistItems = DEFAULT_SYMBOLS.map((sym) => {
    const overview = overviewData?.marketOverview;
    const allSummaries = [
      ...(overview?.topGainers ?? []),
      ...(overview?.topLosers ?? []),
      ...(overview?.mostActive ?? []),
    ];
    const summary = allSummaries.find((s) => s.symbol === sym);

    return {
      symbol: sym,
      price: summary?.closePrice ?? null,
      changePct: summary?.priceChangePct ?? null,
    };
  });

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "240px 1fr",
      gridTemplateRows: "auto 1fr",
      height: "100vh",
      gap: "8px",
      padding: "8px",
      background: "#1a1a2e",
    }}>
      {/* Header — spans full width */}
      <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: "12px" }}>
        <SymbolHeader
          symbol={activeSymbol}
          lastTrade={lastTrade}
          dailySummary={latestSummary}
          tradeCount={tradeCount}
        />
        <ConnectionStatus status={connectionStatus} />
      </div>

      {/* Left sidebar */}
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", overflow: "hidden" }}>
        <Watchlist
          items={watchlistItems}
          activeSymbol={activeSymbol}
          onSelectSymbol={setActiveSymbol}
        />
        <OrderBook symbol={activeSymbol} />
      </div>

      {/* Main chart area */}
      <div style={{ display: "flex", flexDirection: "column", gap: "8px", overflow: "hidden" }}>
        <div style={{ flex: 3, background: "#16213e", borderRadius: "6px", overflow: "hidden" }}>
          {loading ? (
            <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "#888" }}>
              Loading chart data...
            </div>
          ) : (
            <CandlestickChart
              candles={candles}
              liveCandle={liveCandle}
              onVisibleRangeChange={handleRangeChange}
            />
          )}
        </div>
        <div style={{ flex: 1, background: "#16213e", borderRadius: "6px", overflow: "hidden", minHeight: "120px" }}>
          <MacdChart macdData={macdData} visibleRange={visibleRange} />
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Verify build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 4: Run all tests**

```bash
cd services/trading-dashboard && npx vitest run
```

Expected: All tests PASS.

- [ ] **Step 5: Commit app assembly**

```bash
git add services/trading-dashboard/src/App.tsx services/trading-dashboard/src/main.tsx
git commit -m "feat(trading-dashboard): wire up full dashboard layout with all components"
```

---

### Task 12: Dockerfile + Docker Compose integration

**Files:**
- Create: `services/trading-dashboard/Dockerfile`
- Create: `services/trading-dashboard/nginx.conf`
- Modify: `infrastructure/docker-compose/docker-compose.services.yml`
- Modify: `Makefile`

- [ ] **Step 1: Create Dockerfile**

Create `services/trading-dashboard/Dockerfile`:

```dockerfile
# Build stage
FROM node:20-alpine AS build

WORKDIR /app

ARG VITE_GRAPHQL_HTTP_URL=http://localhost:8000/graphql
ARG VITE_GRAPHQL_WS_URL=ws://localhost:8000/graphql

COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi

COPY . .
RUN npm run build

# Production stage
FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
```

- [ ] **Step 2: Create nginx.conf**

Create `services/trading-dashboard/nginx.conf`:

```nginx
server {
    listen 80;
    server_name localhost;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /assets/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

- [ ] **Step 3: Add service to docker-compose.services.yml**

Append to `infrastructure/docker-compose/docker-compose.services.yml` after the `graphql-api` service block (after line 106):

```yaml

  trading-dashboard:
    build:
      context: ../../services/trading-dashboard
      dockerfile: Dockerfile
      args:
        VITE_GRAPHQL_HTTP_URL: http://localhost:8000/graphql
        VITE_GRAPHQL_WS_URL: ws://localhost:8000/graphql
    hostname: trading-dashboard
    container_name: trading-dashboard
    ports:
      - "3002:80"
    networks:
      - de-network
    depends_on:
      graphql-api:
        condition: service_healthy
    restart: unless-stopped
```

- [ ] **Step 4: Add Makefile targets**

Append to `Makefile` before the `# === Logs (per service) ===` section:

```makefile
# === Trading Dashboard ===

build-dashboard: ## Build Trading Dashboard Docker image
	$(COMPOSE_ALL) build trading-dashboard

up-dashboard: build-dashboard ## Start Trading Dashboard (requires graphql-api)
	$(COMPOSE_ALL) up -d trading-dashboard

down-dashboard: ## Stop Trading Dashboard
	$(COMPOSE_ALL) stop trading-dashboard
	$(COMPOSE_ALL) rm -f trading-dashboard

test-dashboard: ## Run Trading Dashboard tests
	cd services/trading-dashboard && npm test

logs-dashboard: ## Tail Trading Dashboard logs
	$(COMPOSE_ALL) logs -f --tail=50 trading-dashboard
```

- [ ] **Step 5: Verify Docker build**

```bash
cd /Users/hpradip/workspace/DE-project && docker build -t trading-dashboard-test services/trading-dashboard/
```

Expected: Build succeeds, image created.

- [ ] **Step 6: Commit Docker integration**

```bash
git add services/trading-dashboard/Dockerfile services/trading-dashboard/nginx.conf infrastructure/docker-compose/docker-compose.services.yml Makefile
git commit -m "feat(trading-dashboard): add Dockerfile, nginx config, docker-compose and Makefile targets"
```

---

### Task 13: Add .gitignore and final cleanup

**Files:**
- Create: `services/trading-dashboard/.gitignore`

- [ ] **Step 1: Create .gitignore**

Create `services/trading-dashboard/.gitignore`:

```
node_modules/
dist/
*.local
```

- [ ] **Step 2: Run full test suite**

```bash
cd services/trading-dashboard && npm test
```

Expected: All tests PASS (MACD tests, candle tests, hook tests).

- [ ] **Step 3: Run build**

```bash
cd services/trading-dashboard && npm run build
```

Expected: Build succeeds.

- [ ] **Step 4: Commit final cleanup**

```bash
git add services/trading-dashboard/.gitignore
git commit -m "chore(trading-dashboard): add .gitignore for node_modules and dist"
```
