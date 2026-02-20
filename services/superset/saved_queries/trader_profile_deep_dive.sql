-- Saved Query: Trader Profile Deep Dive
-- Description: Comprehensive trading profile for a single trader.
-- Tags: trader, profiling, compliance
-- Parameters:
--   {{ trader_id }}  – trader identifier (e.g. T-00234)
--   {{ start_date }} – date range start (e.g. 2024-01-01)
--   {{ end_date }}   – date range end   (e.g. 2024-12-31)

WITH trader_trades AS (
    SELECT *
    FROM trades_enriched
    WHERE trade_date BETWEEN
        '{{ start_date | default("2024-01-01") }}' AND
        '{{ end_date   | default("2024-12-31") }}'
),
symbol_breakdown AS (
    SELECT
        symbol,
        COUNT(*)                                          AS trades,
        SUM(quantity)                                     AS total_qty,
        SUM(total_value)                                  AS total_val,
        COUNT(CASE WHEN trade_type = 'BUY'  THEN 1 END)  AS buy_count,
        COUNT(CASE WHEN trade_type = 'SELL' THEN 1 END)  AS sell_count,
        AVG(price)                                        AS avg_price
    FROM trader_trades
    GROUP BY symbol
),
time_analysis AS (
    SELECT
        trade_hour               AS hour,
        COUNT(*)                 AS trade_count,
        SUM(total_value)         AS hour_value
    FROM trader_trades
    GROUP BY trade_hour
)
SELECT
    (SELECT COUNT(*)         FROM trader_trades) AS total_trades,
    (SELECT SUM(total_value) FROM trader_trades) AS total_value,
    (SELECT COUNT(DISTINCT symbol) FROM trader_trades)    AS unique_symbols,
    (SELECT COUNT(DISTINCT trade_date) FROM trader_trades) AS active_days,
    (SELECT symbol FROM symbol_breakdown ORDER BY total_val DESC LIMIT 1) AS top_symbol,
    (SELECT hour   FROM time_analysis   ORDER BY trade_count DESC LIMIT 1) AS most_active_hour;
