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
      flex: 1,
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
        <span>H: ${dailySummary?.highPrice?.toFixed(2) ?? "\u2014"}</span>
        <span style={{ marginLeft: "12px" }}>L: ${dailySummary?.lowPrice?.toFixed(2) ?? "\u2014"}</span>
        <span style={{ marginLeft: "12px" }}>V: {dailySummary?.totalVolume?.toLocaleString() ?? "\u2014"}</span>
        <span style={{ marginLeft: "12px" }}>Trades: {tradeCount}</span>
      </div>
    </div>
  );
}
