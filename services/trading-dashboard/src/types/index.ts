export interface OhlcvCandle {
  time: string; // ISO date string YYYY-MM-DD
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Trade {
  tradeId: string;
  symbol: string;
  price: number;
  quantity: number;
  timestamp: string;
  isAggressiveBuy: boolean;
}

export interface DailySummaryNode {
  symbol?: string;
  tradingDate: string;
  openPrice: number;
  closePrice: number;
  highPrice: number;
  lowPrice: number;
  totalVolume: number;
  tradeCount: number;
  totalValue: number;
  companyName: string | null;
  priceChange: number | null;
  priceChangePct: number | null;
}

export interface SymbolNode {
  symbol: string;
  companyName: string;
  sector: string;
  marketCapCategory: string;
  isCurrent: boolean;
}

export interface OrderBookSnapshot {
  symbol: string;
  bestBidPrice: number | null;
  bestBidQty: number | null;
  bestAskPrice: number | null;
  bestAskQty: number | null;
  bidDepth: number;
  askDepth: number;
  spread: number | null;
  midPrice: number | null;
}

export interface MarketOverview {
  tradingDate: string;
  totalTrades: number;
  uniqueSymbols: number;
  totalVolume: number;
  totalValue: number;
  advancing: number;
  declining: number;
  unchanged: number;
  topGainers: DailySummaryNode[];
  topLosers: DailySummaryNode[];
  mostActive: DailySummaryNode[];
}

export interface MacdPoint {
  time: string;
  macd: number;
  signal: number;
  histogram: number;
}

export interface PageInfo {
  hasNextPage: boolean;
  endCursor: string | null;
}

export interface Connection<T> {
  edges: Array<{ node: T; cursor: string }>;
  pageInfo: PageInfo;
  totalCount: number;
}
