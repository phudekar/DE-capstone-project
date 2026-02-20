/**
 * transformProps.ts
 * Transforms Superset chart props into ECharts option for the order book depth chart.
 *
 * Expected input columns: price (DOUBLE), quantity (BIGINT), side (VARCHAR: 'BID'|'ASK')
 */

import type { ChartProps } from "@superset-ui/core";

interface DepthRow {
  price: number;
  quantity: number;
  side: "BID" | "ASK" | string;
}

function cumulativeVolume(rows: DepthRow[]): [number, number][] {
  let cum = 0;
  return rows.map(r => {
    cum += r.quantity;
    return [r.price, cum];
  });
}

export default function transformProps(chartProps: ChartProps) {
  const { queriesData, width, height } = chartProps;
  const data: DepthRow[] = (queriesData[0]?.data ?? []).map(
    (r: Record<string, unknown>) => ({
      price: Number(r.price ?? 0),
      quantity: Number(r.quantity ?? 0),
      side: String(r.side ?? ""),
    })
  );

  const bids = data
    .filter(r => r.side === "BID")
    .sort((a, b) => b.price - a.price);   // descending price

  const asks = data
    .filter(r => r.side === "ASK")
    .sort((a, b) => a.price - b.price);   // ascending price

  const bidCum = cumulativeVolume(bids);
  const askCum = cumulativeVolume(asks);

  const echartOptions = {
    xAxis: { type: "value", name: "Price", scale: true },
    yAxis: { type: "value", name: "Cumulative Volume" },
    series: [
      {
        name: "Bids",
        type: "line",
        step: "end",
        areaStyle: { color: "rgba(38, 166, 154, 0.3)" },
        lineStyle: { color: "#26a69a" },
        data: bidCum,
      },
      {
        name: "Asks",
        type: "line",
        step: "start",
        areaStyle: { color: "rgba(239, 83, 80, 0.3)" },
        lineStyle: { color: "#ef5350" },
        data: askCum,
      },
    ],
    tooltip: { trigger: "axis" },
    legend: { data: ["Bids", "Asks"] },
    grid: { left: "10%", right: "5%", bottom: "10%" },
  };

  return { width, height, echartOptions };
}
