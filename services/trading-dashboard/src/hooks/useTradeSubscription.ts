import { useSubscription } from "@apollo/client";
import { useCallback, useRef, useState } from "react";
import { ON_NEW_TRADE_SUBSCRIPTION } from "../graphql/subscriptions";
import { createCandleFromTrade, updateCandleWithTrade } from "../utils/candle";
import type { OhlcvCandle, Trade } from "../types";

export type ConnectionState = "live" | "connecting" | "disconnected";

interface SubscriptionData {
  onNewTrade: Trade;
}

interface TradeSubscriptionResult {
  liveCandle: OhlcvCandle | null;
  lastTrade: Trade | null;
  connectionStatus: ConnectionState;
  tradeCount: number;
}

export function useTradeSubscription(symbol: string): TradeSubscriptionResult {
  const [liveCandle, setLiveCandle] = useState<OhlcvCandle | null>(null);
  const [lastTrade, setLastTrade] = useState<Trade | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionState>("connecting");
  const tradeCountRef = useRef(0);
  const [tradeCount, setTradeCount] = useState(0);

  const onData = useCallback(
    ({ data }: { data?: { data?: SubscriptionData } }) => {
      const trade = data?.data?.onNewTrade;
      if (!trade) return;

      setConnectionStatus("live");
      setLastTrade(trade);
      tradeCountRef.current += 1;
      setTradeCount(tradeCountRef.current);

      setLiveCandle((prev) => {
        if (!prev) return createCandleFromTrade(trade);

        // Compare at minute granularity — new minute = new candle
        const tradeMinute = trade.timestamp.slice(0, 16);
        const candleMinute = prev.time.slice(0, 16);
        if (tradeMinute !== candleMinute) {
          return createCandleFromTrade(trade);
        }

        return updateCandleWithTrade(prev, trade);
      });
    },
    [],
  );

  useSubscription<SubscriptionData>(ON_NEW_TRADE_SUBSCRIPTION, {
    variables: { symbol },
    skip: !symbol,
    onData,
    onError: () => setConnectionStatus("disconnected"),
  });

  return { liveCandle, lastTrade, connectionStatus, tradeCount };
}
