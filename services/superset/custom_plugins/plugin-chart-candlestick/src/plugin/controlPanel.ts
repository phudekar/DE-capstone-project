/**
 * controlPanel.ts
 * Defines the chart configuration UI controls for the candlestick plugin.
 */

import { t } from "@superset-ui/core";

const config = {
  controlPanelSections: [
    {
      label: t("Data Columns"),
      expanded: true,
      controlSetRows: [
        [
          {
            name: "dateColumn",
            config: {
              type: "SelectControl",
              label: t("Date/Time Column"),
              description: t("Column containing the date or timestamp for each bar."),
              mapStateToProps: (state: { datasource?: { columns?: { column_name: string }[] } }) => ({
                choices: (state.datasource?.columns ?? []).map(
                  (c: { column_name: string }) => [c.column_name, c.column_name]
                ),
              }),
              default: "trade_date",
            },
          },
        ],
        [
          { name: "openColumn",  config: { type: "SelectControl", label: t("Open"),  default: "open_price" } },
          { name: "highColumn",  config: { type: "SelectControl", label: t("High"),  default: "high" } },
        ],
        [
          { name: "lowColumn",   config: { type: "SelectControl", label: t("Low"),   default: "low" } },
          { name: "closeColumn", config: { type: "SelectControl", label: t("Close"), default: "close" } },
        ],
        [
          { name: "volumeColumn", config: { type: "SelectControl", label: t("Volume"), default: "total_volume" } },
          { name: "vwapColumn",   config: { type: "SelectControl", label: t("VWAP (optional)"), default: "vwap" } },
        ],
      ],
    },
    {
      label: t("Chart Options"),
      expanded: true,
      controlSetRows: [
        [
          { name: "showVolume", config: { type: "CheckboxControl", label: t("Show Volume Bars"), default: true } },
          { name: "showVwap",   config: { type: "CheckboxControl", label: t("Show VWAP Line"),   default: true } },
        ],
        [
          { name: "bullishColor", config: { type: "ColorPickerControl", label: t("Bullish Color"), default: { r: 38, g: 166, b: 154, a: 1 } } },
          { name: "bearishColor", config: { type: "ColorPickerControl", label: t("Bearish Color"), default: { r: 239, g: 83, b: 80, a: 1 } } },
        ],
      ],
    },
  ],
};

export default config;
