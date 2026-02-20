/**
 * index.ts
 * Plugin registration for the ECharts Candlestick chart.
 */

import { ChartPlugin } from "@superset-ui/core";
import controlPanel from "./plugin/controlPanel";
import transformProps from "./plugin/transformProps";

export default class EchartsCandlestickPlugin extends ChartPlugin {
  constructor() {
    super({
      key: "echarts_candlestick",
      metadata: {
        name: "ECharts Candlestick",
        description: "OHLCV candlestick chart with optional volume bars and VWAP overlay.",
        tags: ["ECharts", "Finance", "Time-series"],
        thumbnail: "",
      },
      loadChart: () =>
        import("./plugin/EchartsCandlestick").then(m => m.default),
      transformProps,
      controlPanel,
    });
  }
}

export { transformProps, controlPanel };
