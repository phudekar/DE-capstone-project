-- Saved Query: Unusual Volume Detection
-- Description: Identifies symbols with volume > 2x their 20-day rolling average.
-- Tags: anomaly, volume, screening

WITH vol_stats AS (
    SELECT
        d.symbol,
        d.trade_date,
        d.total_volume                                                AS volume,
        AVG(d.total_volume) OVER (
            PARTITION BY d.symbol
            ORDER BY d.trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d,
        STDDEV(d.total_volume) OVER (
            PARTITION BY d.symbol
            ORDER BY d.trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS stddev_volume_20d
    FROM daily_trade_summary d
    WHERE d.trade_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    v.symbol,
    s.sector,
    v.trade_date,
    v.volume,
    ROUND(v.avg_volume_20d)                                          AS avg_vol_20d,
    ROUND(v.volume / NULLIF(v.avg_volume_20d, 0), 2)                 AS volume_ratio,
    ROUND((v.volume - v.avg_volume_20d) / NULLIF(v.stddev_volume_20d, 0), 2) AS z_score
FROM vol_stats v
JOIN symbol_reference s ON v.symbol = s.ticker
WHERE v.trade_date = (SELECT MAX(trade_date) FROM daily_trade_summary)
  AND v.volume > 2 * v.avg_volume_20d
ORDER BY volume_ratio DESC;
