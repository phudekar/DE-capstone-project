import { describe, it, expect } from "vitest";
import { transformDailySummaryToCandles } from "./useOhlcvData";
import type { DailySummaryNode } from "../types";

describe("transformDailySummaryToCandles", () => {
  it("converts DailySummaryNode array to OhlcvCandle array", () => {
    const nodes: DailySummaryNode[] = [
      {
        tradingDate: "2026-03-14",
        openPrice: 150.0,
        closePrice: 152.0,
        highPrice: 155.0,
        lowPrice: 149.0,
        totalVolume: 50000,
        tradeCount: 1200,
        totalValue: 7500000,
        companyName: "Apple Inc.",
        priceChange: 2.0,
        priceChangePct: 1.33,
      },
      {
        tradingDate: "2026-03-15",
        openPrice: 152.0,
        closePrice: 148.0,
        highPrice: 153.0,
        lowPrice: 147.0,
        totalVolume: 60000,
        tradeCount: 1500,
        totalValue: 8900000,
        companyName: "Apple Inc.",
        priceChange: -4.0,
        priceChangePct: -2.63,
      },
    ];

    const candles = transformDailySummaryToCandles(nodes);

    expect(candles).toHaveLength(2);
    expect(candles[0]).toEqual({
      time: "2026-03-14",
      open: 150.0,
      high: 155.0,
      low: 149.0,
      close: 152.0,
      volume: 50000,
    });
    expect(candles[1]).toEqual({
      time: "2026-03-15",
      open: 152.0,
      high: 153.0,
      low: 147.0,
      close: 148.0,
      volume: 60000,
    });
  });

  it("returns empty array for empty input", () => {
    expect(transformDailySummaryToCandles([])).toEqual([]);
  });

  it("sorts candles by date ascending", () => {
    const nodes: DailySummaryNode[] = [
      {
        tradingDate: "2026-03-16",
        openPrice: 100, closePrice: 101, highPrice: 102, lowPrice: 99,
        totalVolume: 1000, tradeCount: 10, totalValue: 100000,
        companyName: null, priceChange: null, priceChangePct: null,
      },
      {
        tradingDate: "2026-03-14",
        openPrice: 98, closePrice: 99, highPrice: 100, lowPrice: 97,
        totalVolume: 800, tradeCount: 8, totalValue: 80000,
        companyName: null, priceChange: null, priceChangePct: null,
      },
    ];

    const candles = transformDailySummaryToCandles(nodes);
    expect(candles[0].time).toBe("2026-03-14");
    expect(candles[1].time).toBe("2026-03-16");
  });
});
