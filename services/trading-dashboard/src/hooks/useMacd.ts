import { useMemo } from "react";
import { calculateMACD } from "../utils/macd";
import type { OhlcvCandle, MacdPoint } from "../types";

export function computeMacdFromCandles(candles: OhlcvCandle[]): MacdPoint[] {
  if (candles.length < 26) return [];

  const closePrices = candles.map((c) => c.close);
  const macdResults = calculateMACD(closePrices);

  if (macdResults.length === 0) return [];

  const offset = candles.length - macdResults.length;
  return macdResults.map((result, i) => ({
    time: candles[i + offset].time,
    macd: result.macd,
    signal: result.signal,
    histogram: result.histogram,
  }));
}

export function useMacd(candles: OhlcvCandle[]): MacdPoint[] {
  return useMemo(() => computeMacdFromCandles(candles), [candles]);
}
