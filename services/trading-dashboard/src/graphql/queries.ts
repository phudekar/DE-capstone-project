import { gql } from "@apollo/client";

export const DAILY_SUMMARY_QUERY = gql`
  query DailySummary($symbol: String!, $dateRange: DateRangeInput!, $first: Int, $after: String) {
    dailySummary(symbol: $symbol, dateRange: $dateRange, first: $first, after: $after) {
      edges {
        node {
          tradingDate
          openPrice
          closePrice
          highPrice
          lowPrice
          totalVolume
          tradeCount
          totalValue
          companyName
          priceChange
          priceChangePct
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
    }
  }
`;

export const MINUTE_CANDLES_QUERY = gql`
  query MinuteCandles($symbol: String!, $dateRange: DateRangeInput!, $interval: String, $first: Int, $after: String) {
    minuteCandles(symbol: $symbol, dateRange: $dateRange, interval: $interval, first: $first, after: $after) {
      edges {
        node {
          symbol
          timestamp
          openPrice
          closePrice
          highPrice
          lowPrice
          totalVolume
          tradeCount
          priceChange
          priceChangePct
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
    }
  }
`;

export const SYMBOLS_QUERY = gql`
  query Symbols($first: Int, $after: String) {
    symbols(first: $first, after: $after) {
      edges {
        node {
          symbol
          companyName
          sector
          marketCapCategory
          isCurrent
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
      totalCount
    }
  }
`;

export const MARKET_OVERVIEW_QUERY = gql`
  query MarketOverview($targetDate: Date) {
    marketOverview(targetDate: $targetDate) {
      tradingDate
      totalTrades
      uniqueSymbols
      totalVolume
      totalValue
      advancing
      declining
      unchanged
      topGainers {
        symbol
        tradingDate
        openPrice
        closePrice
        highPrice
        lowPrice
        totalVolume
        companyName
        priceChange
        priceChangePct
      }
      topLosers {
        symbol
        tradingDate
        openPrice
        closePrice
        highPrice
        lowPrice
        totalVolume
        companyName
        priceChange
        priceChangePct
      }
      mostActive {
        symbol
        tradingDate
        openPrice
        closePrice
        totalVolume
        companyName
        priceChangePct
      }
    }
  }
`;

export const ORDER_BOOK_QUERY = gql`
  query OrderBook($symbol: String!) {
    orderBook(symbol: $symbol) {
      symbol
      bestBidPrice
      bestBidQty
      bestAskPrice
      bestAskQty
      bidDepth
      askDepth
      spread
      midPrice
    }
  }
`;

export const WATCHLIST_PRICE_QUERY = gql`
  query WatchlistPrice($symbol: String!, $dateRange: DateRangeInput!) {
    dailySummary(symbol: $symbol, dateRange: $dateRange, first: 1) {
      edges {
        node {
          closePrice
          priceChangePct
        }
      }
    }
  }
`;
