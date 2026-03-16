import { useEffect, useRef } from "react";
import {
  createChart,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type Time,
  ColorType,
  CrosshairMode,
} from "lightweight-charts";
import type { OhlcvCandle } from "../types";

interface Props {
  candles: OhlcvCandle[];
  liveCandle: OhlcvCandle | null;
  onVisibleRangeChange?: (range: { from: number; to: number } | null) => void;
}

/** Convert ISO time string to Lightweight Charts Time.
 *  - "YYYY-MM-DD" → pass as-is (daily)
 *  - Full datetime → Unix timestamp in seconds (intraday)
 */
function toChartTime(timeStr: string): Time {
  if (timeStr.length <= 10) {
    // Date-only string like "2026-03-16"
    return timeStr as Time;
  }
  // Full datetime — convert to Unix timestamp (seconds)
  return Math.floor(new Date(timeStr).getTime() / 1000) as unknown as Time;
}

function isIntraday(candles: OhlcvCandle[]): boolean {
  if (candles.length === 0) return false;
  return candles[0].time.length > 10;
}

function toCandlestickData(candle: OhlcvCandle): CandlestickData<Time> {
  return {
    time: toChartTime(candle.time),
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

function toVolumeData(candle: OhlcvCandle): HistogramData<Time> {
  return {
    time: toChartTime(candle.time),
    value: candle.volume,
    color: candle.close >= candle.open ? "rgba(38, 166, 154, 0.4)" : "rgba(239, 83, 80, 0.4)",
  };
}

export default function CandlestickChart({ candles, liveCandle, onVisibleRangeChange }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#1a1a2e" },
        textColor: "#888",
      },
      grid: {
        vertLines: { color: "#2a2a4a" },
        horzLines: { color: "#2a2a4a" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#2a2a4a" },
      timeScale: { borderColor: "#2a2a4a", timeVisible: true, secondsVisible: false },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#26a69a",
      downColor: "#ef5350",
      borderDownColor: "#ef5350",
      borderUpColor: "#26a69a",
      wickDownColor: "#ef5350",
      wickUpColor: "#26a69a",
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });

    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    if (onVisibleRangeChange) {
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        onVisibleRangeChange(range ? { from: range.from, to: range.to } : null);
      });
    }

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    const resizeObserver = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      chart.applyOptions({ width, height });
    });
    resizeObserver.observe(containerRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, []);

  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || candles.length === 0) return;

    // Update timeScale visibility based on whether data is intraday
    const intraday = isIntraday(candles);
    chartRef.current?.timeScale().applyOptions({
      timeVisible: intraday,
      secondsVisible: false,
    });

    candleSeriesRef.current.setData(candles.map(toCandlestickData));
    volumeSeriesRef.current.setData(candles.map(toVolumeData));
    chartRef.current?.timeScale().fitContent();
  }, [candles]);

  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || !liveCandle) return;
    candleSeriesRef.current.update(toCandlestickData(liveCandle));
    volumeSeriesRef.current.update(toVolumeData(liveCandle));
  }, [liveCandle]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", minHeight: "300px" }}
    />
  );
}
