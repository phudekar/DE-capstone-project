/**
 * Types for the ECharts Candlestick chart plugin.
 */

export interface CandlestickRow {
  date: string;
  open: number;
  close: number;
  high: number;
  low: number;
  volume: number;
  vwap?: number;
  change_pct?: number;
}

export interface CandlestickFormData {
  dateColumn: string;
  openColumn: string;
  closeColumn: string;
  highColumn: string;
  lowColumn: string;
  volumeColumn: string;
  vwapColumn?: string;
  showVolume: boolean;
  showVwap: boolean;
  bullishColor: string;
  bearishColor: string;
  colorScheme?: string;
}

export interface CandlestickChartProps {
  width: number;
  height: number;
  data: CandlestickRow[];
  formData: CandlestickFormData;
}
