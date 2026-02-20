-- Saved Query: Symbol OHLCV with Technical Indicators (SMA-20, SMA-50, RSI-14)
-- Description: Daily OHLCV bars enriched with moving averages and RSI.
-- Tags: technical-analysis, daily, ohlcv
-- Parameters: {{ symbol }} – ticker symbol (e.g. AAPL)

WITH daily AS (
    SELECT
        trade_date,
        symbol,
        open_price,
        high,
        low,
        close,
        total_volume     AS volume,
        vwap,
        -- Simple Moving Averages
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        -- Price change for RSI calculation
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_change
    FROM daily_trade_summary
    WHERE symbol = '{{ symbol | default("AAPL") }}'
      AND trade_date >= CURRENT_DATE - INTERVAL '180 days'
),
rsi_calc AS (
    SELECT
        *,
        AVG(CASE WHEN price_change > 0 THEN price_change ELSE 0 END) OVER (
            ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END) OVER (
            ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM daily
)
SELECT
    trade_date,
    symbol,
    open_price,
    high,
    low,
    close,
    volume,
    vwap,
    ROUND(sma_20, 2) AS sma_20,
    ROUND(sma_50, 2) AS sma_50,
    ROUND(100 - (100 / (1 + avg_gain / NULLIF(avg_loss, 0))), 2) AS rsi_14
FROM rsi_calc
ORDER BY trade_date DESC;
