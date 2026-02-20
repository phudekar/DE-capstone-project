-- region_filter_view.sql
-- Row-Level Security: restrict gold summary rows by the requesting user's region.
--
-- Assumes gold.daily_trading_summary has a `region` column (populated during
-- gold layer aggregation). The requesting user's region is set as a DuckDB
-- session variable before querying.
--
-- Usage (from Python / DuckDB):
--   conn.execute("SET user_region = ?", [current_user.region])
--   conn.execute("SELECT * FROM daily_summary_rls_region")

CREATE OR REPLACE VIEW daily_summary_rls_region AS
SELECT
    symbol,
    trade_date,
    region,
    total_volume,
    vwap,
    open_price,
    close_price,
    high_price,
    low_price,
    trade_count,
    avg_bid_ask_spread
FROM gold.daily_trading_summary
WHERE region = current_setting('user_region')
   OR current_setting('user_region') = 'GLOBAL';   -- GLOBAL role sees all regions


-- Top-movers scoped to the user's region
CREATE OR REPLACE VIEW top_movers_rls_region AS
SELECT
    symbol,
    trade_date,
    region,
    price_change_pct,
    rank
FROM gold.top_movers
WHERE region = current_setting('user_region')
   OR current_setting('user_region') = 'GLOBAL';
