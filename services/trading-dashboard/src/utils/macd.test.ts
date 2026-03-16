import { describe, it, expect } from "vitest";
import { calculateEMA, calculateMACD } from "./macd";

describe("calculateEMA", () => {
  it("returns empty array for insufficient data", () => {
    expect(calculateEMA([1, 2], 3)).toEqual([]);
  });

  it("seeds with SMA for first value", () => {
    const prices = [10, 11, 12, 13, 14];
    const result = calculateEMA(prices, 3);
    expect(result[0]).toBeCloseTo(11, 5);
  });

  it("calculates EMA correctly for period 3", () => {
    const prices = [10, 11, 12, 13, 14];
    const result = calculateEMA(prices, 3);
    expect(result).toHaveLength(3);
    expect(result[0]).toBeCloseTo(11, 5);
    expect(result[1]).toBeCloseTo(12, 5);
    expect(result[2]).toBeCloseTo(13, 5);
  });

  it("returns values aligned to end of input array", () => {
    const prices = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    const result = calculateEMA(prices, 5);
    expect(result).toHaveLength(6);
  });
});

describe("calculateMACD", () => {
  it("returns empty for insufficient data", () => {
    const prices = Array.from({ length: 20 }, (_, i) => 100 + i);
    const result = calculateMACD(prices);
    expect(result.length).toBeLessThanOrEqual(0);
  });

  it("returns correct structure for sufficient data", () => {
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
