import type { OhlcvCandle, Trade } from "../types";

function toDateString(timestamp: string): string {
  return timestamp.slice(0, 10);
}

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
