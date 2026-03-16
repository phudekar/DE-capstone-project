import { describe, it, expect } from "vitest";
import { transformMinuteCandlesToOhlcv } from "./useOhlcvData";
import type { MinuteCandleNode } from "../types";

describe("transformMinuteCandlesToOhlcv", () => {
  it("converts MinuteCandleNode array to OhlcvCandle array", () => {
    const nodes: MinuteCandleNode[] = [
      {
        symbol: "AAPL",
        timestamp: "2026-03-16T10:00:00",
        openPrice: 150.0,
        closePrice: 152.0,
        highPrice: 155.0,
        lowPrice: 149.0,
        totalVolume: 50000,
        tradeCount: 1200,
        priceChange: 2.0,
        priceChangePct: 1.33,
      },
      {
        symbol: "AAPL",
        timestamp: "2026-03-16T10:01:00",
        openPrice: 152.0,
        closePrice: 148.0,
        highPrice: 153.0,
        lowPrice: 147.0,
        totalVolume: 60000,
        tradeCount: 1500,
        priceChange: -4.0,
        priceChangePct: -2.63,
      },
    ];

    const candles = transformMinuteCandlesToOhlcv(nodes);

    expect(candles).toHaveLength(2);
    expect(candles[0]).toEqual({
      time: "2026-03-16T10:00:00",
      open: 150.0,
      high: 155.0,
      low: 149.0,
      close: 152.0,
      volume: 50000,
    });
    expect(candles[1]).toEqual({
      time: "2026-03-16T10:01:00",
      open: 152.0,
      high: 153.0,
      low: 147.0,
      close: 148.0,
      volume: 60000,
    });
  });

  it("returns empty array for empty input", () => {
    expect(transformMinuteCandlesToOhlcv([])).toEqual([]);
  });

  it("sorts candles by timestamp ascending", () => {
    const nodes: MinuteCandleNode[] = [
      {
        symbol: "AAPL",
        timestamp: "2026-03-16T10:05:00",
        openPrice: 100, closePrice: 101, highPrice: 102, lowPrice: 99,
        totalVolume: 1000, tradeCount: 10,
        priceChange: null, priceChangePct: null,
      },
      {
        symbol: "AAPL",
        timestamp: "2026-03-16T10:01:00",
        openPrice: 98, closePrice: 99, highPrice: 100, lowPrice: 97,
        totalVolume: 800, tradeCount: 8,
        priceChange: null, priceChangePct: null,
      },
    ];

    const candles = transformMinuteCandlesToOhlcv(nodes);
    expect(candles[0].time).toBe("2026-03-16T10:01:00");
    expect(candles[1].time).toBe("2026-03-16T10:05:00");
  });
});
