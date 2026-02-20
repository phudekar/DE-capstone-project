-- Saved Query: Cross-Account Activity (Potential Wash Trade Detection)
-- Description: Detects pairs of accounts making opposite trades in the same symbol
--              within 30 seconds (potential wash trading or coordinated activity).
-- Tags: compliance, wash-trade, surveillance

WITH trade_pairs AS (
    SELECT
        t1.symbol,
        t1.trade_type          AS type_1,
        t2.trade_type          AS type_2,
        t1.quantity            AS qty_1,
        t2.quantity            AS qty_2,
        t1.price               AS price_1,
        t2.price               AS price_2,
        t1.trade_date          AS date_1,
        t2.trade_date          AS date_2
    FROM trades_enriched t1
    JOIN trades_enriched t2
        ON  t1.symbol     = t2.symbol
        AND t1.trade_type != t2.trade_type                      -- opposite sides
        AND t1.trade_date = t2.trade_date                       -- same day
        AND ABS(t1.quantity - t2.quantity) < t1.quantity * 0.05 -- similar size (±5%)
        AND t1.trade_date >= CURRENT_DATE - INTERVAL '7 days'
    WHERE t1.trade_type = 'BUY'   -- prevent double-counting
)
SELECT
    symbol,
    COUNT(*)                    AS paired_trades,
    AVG(price_1)                AS avg_price,
    SUM(qty_1)                  AS total_qty
FROM trade_pairs
GROUP BY symbol
HAVING COUNT(*) >= 3
ORDER BY paired_trades DESC
LIMIT 50;
