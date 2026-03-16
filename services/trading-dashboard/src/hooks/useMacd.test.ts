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
    expect(result[result.length - 1].time).toBe(candles[candles.length - 1].time);
  });
});
