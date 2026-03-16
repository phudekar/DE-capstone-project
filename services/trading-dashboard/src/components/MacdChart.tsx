import { useEffect, useRef } from "react";
import {
  createChart,
  type IChartApi,
  type ISeriesApi,
  type HistogramData,
  type LineData,
  type Time,
  ColorType,
  CrosshairMode,
} from "lightweight-charts";
import type { MacdPoint } from "../types";

interface Props {
  macdData: MacdPoint[];
  visibleRange?: { from: number; to: number } | null;
}

export default function MacdChart({ macdData, visibleRange }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const macdLineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const signalLineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const histogramRef = useRef<ISeriesApi<"Histogram"> | null>(null);

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
      timeScale: { borderColor: "#2a2a4a", timeVisible: false },
    });

    const histogram = chart.addHistogramSeries({
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    const macdLine = chart.addLineSeries({
      color: "#2196f3",
      lineWidth: 2,
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    const signalLine = chart.addLineSeries({
      color: "#ff9800",
      lineWidth: 2,
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
    });

    chartRef.current = chart;
    macdLineRef.current = macdLine;
    signalLineRef.current = signalLine;
    histogramRef.current = histogram;

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
    if (!macdLineRef.current || !signalLineRef.current || !histogramRef.current) return;
    if (macdData.length === 0) return;

    const macdLineData: LineData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.macd,
    }));

    const signalLineData: LineData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.signal,
    }));

    const histogramData: HistogramData<Time>[] = macdData.map((p) => ({
      time: p.time as Time,
      value: p.histogram,
      color: p.histogram >= 0 ? "#26a69a" : "#ef5350",
    }));

    histogramRef.current.setData(histogramData);
    macdLineRef.current.setData(macdLineData);
    signalLineRef.current.setData(signalLineData);
  }, [macdData]);

  useEffect(() => {
    if (!chartRef.current || !visibleRange) return;
    chartRef.current.timeScale().setVisibleLogicalRange({
      from: visibleRange.from,
      to: visibleRange.to,
    });
  }, [visibleRange]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: "100%", minHeight: "120px" }}
    />
  );
}
