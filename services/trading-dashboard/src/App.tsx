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
import { MARKET_OVERVIEW_QUERY } from "./graphql/queries";
import type { MarketOverview } from "./types";

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
