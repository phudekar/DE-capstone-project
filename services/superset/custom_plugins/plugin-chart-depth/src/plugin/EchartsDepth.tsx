/**
 * EchartsDepth.tsx
 * React component that renders the order book depth chart.
 */

import React from "react";
import ReactECharts from "echarts-for-react";
import transformProps from "./transformProps";

interface Props {
  width: number;
  height: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  chartProps: any;
}

export default function EchartsDepth({ width, height, chartProps }: Props) {
  const { echartOptions } = transformProps({ ...chartProps, width, height });

  return (
    <ReactECharts
      option={echartOptions}
      style={{ width, height }}
      notMerge
      lazyUpdate
    />
  );
}
