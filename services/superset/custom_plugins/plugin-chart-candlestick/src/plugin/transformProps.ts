/**
 * transformProps.ts
 * Transforms Superset chart props into ECharts option for the candlestick chart.
 */

import type { ChartProps } from "@superset-ui/core";
import type { CandlestickRow, CandlestickFormData } from "../types";

export default function transformProps(chartProps: ChartProps) {
  const { queriesData, formData, width, height } = chartProps;
  const fd = formData as CandlestickFormData;
  const data = queriesData[0]?.data ?? [];

  const rows: CandlestickRow[] = data.map((row: Record<string, unknown>) => ({
    date:       String(row[fd.dateColumn] ?? ""),
    open:       Number(row[fd.openColumn] ?? 0),
    close:      Number(row[fd.closeColumn] ?? 0),
    high:       Number(row[fd.highColumn] ?? 0),
    low:        Number(row[fd.lowColumn] ?? 0),
    volume:     Number(row[fd.volumeColumn] ?? 0),
    vwap:       fd.vwapColumn ? Number(row[fd.vwapColumn] ?? 0) : undefined,
    change_pct: row.change_pct != null ? Number(row.change_pct) : undefined,
  }));

  const bullish = fd.bullishColor ?? "#26a69a";
  const bearish = fd.bearishColor ?? "#ef5350";

  const series: object[] = [
    {
      name: "OHLC",
      type: "candlestick",
      data: rows.map(r => [r.open, r.close, r.low, r.high]),
      itemStyle: {
        color: bullish,
        color0: bearish,
        borderColor: bullish,
        borderColor0: bearish,
      },
    },
  ];

  if (fd.showVolume) {
    series.push({
      name: "Volume",
      type: "bar",
      yAxisIndex: 1,
      data: rows.map(r => r.volume),
      itemStyle: { color: "#90caf9", opacity: 0.5 },
    });
  }

  if (fd.showVwap && fd.vwapColumn) {
    series.push({
      name: "VWAP",
      type: "line",
      data: rows.map(r => r.vwap ?? null),
      lineStyle: { color: "#ff9800", width: 1.5, type: "dashed" },
      symbol: "none",
    });
  }

  const echartOptions: object = {
    xAxis: {
      type: "category",
      data: rows.map(r => r.date),
      axisLabel: { rotate: 30 },
    },
    yAxis: [
      { type: "value", name: "Price", scale: true },
      fd.showVolume
        ? { type: "value", name: "Volume", position: "right", scale: true }
        : {},
    ],
    series,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
    },
    legend: { data: ["OHLC", "Volume", "VWAP"] },
    dataZoom: [
      { type: "inside", start: 0, end: 100 },
      { type: "slider", start: 70, end: 100 },
    ],
    grid: { left: "10%", right: "10%", bottom: "15%" },
  };

  return { width, height, echartOptions, rows };
}
