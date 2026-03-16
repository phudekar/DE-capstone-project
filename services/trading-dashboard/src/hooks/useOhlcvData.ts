import { useQuery } from "@apollo/client";
import { useMemo } from "react";
import { DAILY_SUMMARY_QUERY } from "../graphql/queries";
import type { OhlcvCandle, DailySummaryNode, Connection } from "../types";

export function transformDailySummaryToCandles(nodes: DailySummaryNode[]): OhlcvCandle[] {
  return nodes
    .map((node) => ({
      time: node.tradingDate,
      open: node.openPrice,
      high: node.highPrice,
      low: node.lowPrice,
      close: node.closePrice,
      volume: node.totalVolume,
    }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

interface DailySummaryData {
  dailySummary: Connection<DailySummaryNode>;
}

export function useOhlcvData(symbol: string) {
  const today = new Date().toISOString().slice(0, 10);
  const oneYearAgo = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const { data, loading, error, fetchMore } = useQuery<DailySummaryData>(DAILY_SUMMARY_QUERY, {
    variables: {
      symbol,
      dateRange: { start: oneYearAgo, end: today },
      first: 365,
    },
    skip: !symbol,
  });

  const candles = useMemo(() => {
    if (!data?.dailySummary?.edges) return [];
    const nodes = data.dailySummary.edges.map((edge) => edge.node);
    return transformDailySummaryToCandles(nodes);
  }, [data]);

  const latestSummary = useMemo(() => {
    if (!data?.dailySummary?.edges?.length) return null;
    const nodes = data.dailySummary.edges.map((e) => e.node);
    return nodes[nodes.length - 1];
  }, [data]);

  return { candles, loading, error, latestSummary, fetchMore };
}
