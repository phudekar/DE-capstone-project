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
            <span>{book.bestBidPrice?.toFixed(2) ?? "\u2014"}</span>
            <span>{book.bestBidQty ?? "\u2014"}</span>
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
            Spread: {book.spread?.toFixed(2) ?? "\u2014"} | Mid: {book.midPrice?.toFixed(2) ?? "\u2014"}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", color: "#ef5350", padding: "3px 0" }}>
            <span>ASK</span>
            <span>{book.bestAskPrice?.toFixed(2) ?? "\u2014"}</span>
            <span>{book.bestAskQty ?? "\u2014"}</span>
          </div>
          <div style={{ marginTop: "8px", fontSize: "10px", color: "#555" }}>
            Bid depth: {book.bidDepth} | Ask depth: {book.askDepth}
          </div>
        </div>
      )}
    </div>
  );
}
