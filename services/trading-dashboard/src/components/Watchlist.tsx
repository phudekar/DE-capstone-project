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
