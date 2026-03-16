import type { OhlcvCandle, Trade } from "../types";

/** Truncate a timestamp to the start of its minute: "2026-03-16T20:08:45+00:00" → "2026-03-16T20:08:00" */
function toMinuteTimestamp(timestamp: string): string {
  // Handle both "2026-03-16T20:08:45" and "2026-03-16T20:08:45+00:00"
  const withoutTz = timestamp.replace(/[+-]\d{2}:\d{2}$/, "").replace(/Z$/, "");
  return withoutTz.slice(0, 16) + ":00";
}

export function createCandleFromTrade(trade: Trade): OhlcvCandle {
  return {
    time: toMinuteTimestamp(trade.timestamp),
    open: trade.price,
    high: trade.price,
    low: trade.price,
    close: trade.price,
    volume: trade.quantity,
  };
}

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
