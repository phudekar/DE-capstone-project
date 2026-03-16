import { gql } from "@apollo/client";

export const ON_NEW_TRADE_SUBSCRIPTION = gql`
  subscription OnNewTrade($symbol: String) {
    onNewTrade(symbol: $symbol) {
      tradeId
      symbol
      price
      quantity
      timestamp
      isAggressiveBuy
    }
  }
`;
