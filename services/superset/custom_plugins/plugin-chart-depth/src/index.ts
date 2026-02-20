/**
 * index.ts
 * Plugin registration for the Order Book Depth chart.
 */

import { ChartPlugin } from "@superset-ui/core";
import transformProps from "./plugin/transformProps";

export default class EchartsDepthPlugin extends ChartPlugin {
  constructor() {
    super({
      key: "echarts_depth",
      metadata: {
        name: "Order Book Depth",
        description: "Cumulative bid/ask volume chart for order book depth visualisation.",
        tags: ["ECharts", "Finance", "Order Book"],
        thumbnail: "",
      },
      loadChart: () =>
        import("./plugin/EchartsDepth").then(m => m.default),
      transformProps,
    });
  }
}

export { transformProps };
