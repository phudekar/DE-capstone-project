import { useQuery } from "@apollo/client";
import { useMemo } from "react";
import { MINUTE_CANDLES_QUERY } from "../graphql/queries";
import type { OhlcvCandle, MinuteCandleNode, Connection, TimeframeInterval } from "../types";

/** How many days of data to fetch for each interval */
const INTERVAL_LOOKBACK_DAYS: Record<TimeframeInterval, number> = {
  "1m": 1,
  "5m": 3,
  "15m": 7,
  "30m": 14,
  "1h": 30,
  "4h": 90,
  "1d": 365,
  "1w": 730,
};

/** Max candles to request per interval */
const INTERVAL_FIRST: Record<TimeframeInterval, number> = {
  "1m": 1440,
  "5m": 1000,
  "15m": 700,
  "30m": 700,
  "1h": 720,
  "4h": 540,
  "1d": 365,
  "1w": 104,
};

export function transformMinuteCandlesToOhlcv(nodes: MinuteCandleNode[]): OhlcvCandle[] {
  return nodes
    .map((node) => ({
      time: node.timestamp,
      open: node.openPrice,
      high: node.highPrice,
      low: node.lowPrice,
      close: node.closePrice,
      volume: node.totalVolume,
    }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

interface MinuteCandlesData {
  minuteCandles: Connection<MinuteCandleNode>;
}

export function useOhlcvData(symbol: string, interval: TimeframeInterval = "1m") {
  const today = new Date().toISOString().slice(0, 10);
  const lookbackDays = INTERVAL_LOOKBACK_DAYS[interval];
  const startDate = new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const { data, loading, error, fetchMore } = useQuery<MinuteCandlesData>(MINUTE_CANDLES_QUERY, {
    variables: {
      symbol,
      dateRange: { start: startDate, end: today },
      interval,
      first: INTERVAL_FIRST[interval],
    },
    skip: !symbol,
    fetchPolicy: "cache-and-network",
  });

  const candles = useMemo(() => {
    if (!data?.minuteCandles?.edges) return [];
    const nodes = data.minuteCandles.edges.map((edge) => edge.node);
    return transformMinuteCandlesToOhlcv(nodes);
  }, [data]);

  const latestSummary = useMemo(() => {
    if (!data?.minuteCandles?.edges?.length) return null;
    const nodes = data.minuteCandles.edges.map((e) => e.node);
    return nodes[nodes.length - 1];
  }, [data]);

  return { candles, loading, error, latestSummary, fetchMore };
}
