-- Saved Query: Daily Market Summary
-- Description: End-of-day market-wide summary — total volume, total value,
--              advance/decline ratio, top gainers & losers for a given date.
-- Tags: market, summary, eod

WITH base AS (
    SELECT
        symbol,
        SUM(quantity)                                       AS total_volume,
        SUM(quantity * price)                               AS total_value,
        MIN(price)                                          AS day_low,
        MAX(price)                                          AS day_high,
        (ARRAY_AGG(price ORDER BY traded_at ASC))[1]        AS open_price,
        (ARRAY_AGG(price ORDER BY traded_at DESC))[1]       AS close_price,
        COUNT(*)                                            AS trade_count
    FROM trades_enriched
    WHERE trade_date = {{ from_dttm | default('CURRENT_DATE - INTERVAL \'1 day\'') }}
    GROUP BY symbol
),
with_change AS (
    SELECT
        b.*,
        close_price - open_price                            AS price_change,
        CASE
            WHEN open_price > 0
            THEN ROUND((close_price - open_price) / open_price * 100, 2)
            ELSE 0
        END                                                 AS pct_change,
        CASE
            WHEN close_price >= open_price THEN 'ADVANCE'
            ELSE 'DECLINE'
        END                                                 AS direction
    FROM base b
),
market_stats AS (
    SELECT
        COUNT(*)                                            AS total_symbols,
        SUM(total_volume)                                   AS market_volume,
        SUM(total_value)                                    AS market_value,
        SUM(trade_count)                                    AS market_trades,
        SUM(CASE WHEN direction = 'ADVANCE' THEN 1 ELSE 0 END) AS advances,
        SUM(CASE WHEN direction = 'DECLINE' THEN 1 ELSE 0 END) AS declines
    FROM with_change
)
-- Per-symbol rows with market-wide context appended
SELECT
    wc.symbol,
    wc.open_price,
    wc.close_price,
    wc.day_low,
    wc.day_high,
    wc.price_change,
    wc.pct_change,
    wc.direction,
    wc.total_volume,
    ROUND(wc.total_value, 2)                                AS total_value,
    wc.trade_count,
    ms.total_symbols,
    ms.market_volume,
    ROUND(ms.market_value, 2)                               AS market_value,
    ms.market_trades,
    ms.advances,
    ms.declines,
    ROUND(ms.advances::DOUBLE / NULLIF(ms.declines, 0), 2)  AS advance_decline_ratio
FROM with_change wc
CROSS JOIN market_stats ms
ORDER BY ABS(wc.pct_change) DESC
LIMIT 100;
