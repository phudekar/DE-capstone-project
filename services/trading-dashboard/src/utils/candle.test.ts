import { describe, it, expect } from "vitest";
import { createCandleFromTrade, updateCandleWithTrade } from "./candle";
import type { OhlcvCandle, Trade } from "../types";

const makeTrade = (overrides: Partial<Trade> = {}): Trade => ({
  tradeId: "t1",
  symbol: "AAPL",
  price: 150.0,
  quantity: 100,
  timestamp: "2026-03-16T10:30:45+00:00",
  isAggressiveBuy: true,
  ...overrides,
});

describe("createCandleFromTrade", () => {
  it("creates a candle with OHLC all set to trade price", () => {
    const trade = makeTrade({ price: 150.0, quantity: 100 });
    const candle = createCandleFromTrade(trade);
    expect(candle.open).toBe(150.0);
    expect(candle.high).toBe(150.0);
    expect(candle.low).toBe(150.0);
    expect(candle.close).toBe(150.0);
    expect(candle.volume).toBe(100);
  });

  it("truncates timestamp to minute boundary", () => {
    const trade = makeTrade({ timestamp: "2026-03-16T10:30:45+00:00" });
    const candle = createCandleFromTrade(trade);
    expect(candle.time).toBe("2026-03-16T10:30:00");
  });

  it("handles Z timezone suffix", () => {
    const trade = makeTrade({ timestamp: "2026-03-16T10:30:45Z" });
    const candle = createCandleFromTrade(trade);
    expect(candle.time).toBe("2026-03-16T10:30:00");
  });

  it("handles timestamp without timezone", () => {
    const trade = makeTrade({ timestamp: "2026-03-16T10:30:45" });
    const candle = createCandleFromTrade(trade);
    expect(candle.time).toBe("2026-03-16T10:30:00");
  });
});

describe("updateCandleWithTrade", () => {
  const baseCandle: OhlcvCandle = {
    time: "2026-03-16T10:30:00",
    open: 150.0,
    high: 155.0,
    low: 148.0,
    close: 152.0,
    volume: 1000,
  };

  it("updates close to latest trade price", () => {
    const trade = makeTrade({ price: 153.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.close).toBe(153.0);
  });

  it("updates high when trade price exceeds current high", () => {
    const trade = makeTrade({ price: 160.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.high).toBe(160.0);
  });

  it("updates low when trade price is below current low", () => {
    const trade = makeTrade({ price: 145.0, quantity: 50 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.low).toBe(145.0);
  });

  it("accumulates volume", () => {
    const trade = makeTrade({ quantity: 200 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.volume).toBe(1200);
  });

  it("does not change open", () => {
    const trade = makeTrade({ price: 999.0 });
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.open).toBe(150.0);
  });

  it("preserves time", () => {
    const trade = makeTrade();
    const updated = updateCandleWithTrade(baseCandle, trade);
    expect(updated.time).toBe("2026-03-16T10:30:00");
  });
});
