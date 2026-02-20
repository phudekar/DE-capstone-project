-- Saved Query: Sector Rotation Analysis
-- Description: Weekly sector net inflow/outflow and average return over 90 days.
-- Tags: sector, rotation, flow-analysis

SELECT
    DATE_TRUNC('week', d.trade_date)                                         AS week,
    s.sector,
    SUM(CASE WHEN d.change_pct > 0 THEN d.total_value_traded ELSE 0 END)    AS net_inflow,
    SUM(CASE WHEN d.change_pct < 0 THEN d.total_value_traded ELSE 0 END)    AS net_outflow,
    SUM((d.change_pct > 0)::INT - (d.change_pct < 0)::INT)
        * AVG(d.total_volume)                                                AS net_volume_flow,
    COUNT(DISTINCT d.symbol)                                                 AS active_symbols,
    ROUND(AVG(d.change_pct), 2)                                              AS avg_return_pct
FROM daily_trade_summary d
JOIN symbol_reference s ON d.symbol = s.ticker
WHERE d.trade_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY 1, 2
ORDER BY 1 DESC, net_volume_flow DESC;
