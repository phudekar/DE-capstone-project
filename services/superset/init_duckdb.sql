-- init_duckdb.sql
-- Executed on each new DuckDB connection to register Iceberg tables as views
-- for Superset dataset discovery and SQL Lab exploration.
--
-- This script is referenced in create_databases.py and run via the DuckDB
-- SQLAlchemy connection's "on_connect" hook.

INSTALL iceberg;
LOAD iceberg;

-- ─── Attach Iceberg REST catalog ─────────────────────────────────────────────

ATTACH 'rest' AS iceberg_catalog (
    TYPE ICEBERG,
    ENDPOINT 'http://iceberg-rest:8181'
);

-- ─── Gold layer views (primary analytics surface) ────────────────────────────

CREATE OR REPLACE VIEW daily_trade_summary AS
SELECT
    symbol,
    trade_date,
    total_volume,
    vwap,
    open_price,
    close_price        AS close,
    high_price         AS high,
    low_price          AS low,
    trade_count,
    avg_bid_ask_spread,
    -- Computed columns for dashboard convenience
    ROUND((close_price - open_price) / NULLIF(open_price, 0) * 100, 4) AS change_pct,
    total_volume * vwap AS total_value_traded
FROM iceberg_catalog.gold.daily_trading_summary;


CREATE OR REPLACE VIEW top_movers AS
SELECT
    symbol,
    trade_date,
    change_pct,
    total_volume,
    vwap
FROM iceberg_catalog.gold.top_movers;


-- ─── Silver layer views (detailed / intraday queries) ────────────────────────

CREATE OR REPLACE VIEW trades_enriched AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side                 AS trade_type,
    trade_date,
    -- Convenience columns
    price * quantity     AS total_value,
    EXTRACT(HOUR FROM trade_date::TIMESTAMP) AS trade_hour
FROM iceberg_catalog.silver.trades;


CREATE OR REPLACE VIEW order_book_snapshots AS
SELECT *
FROM iceberg_catalog.silver.ohlcv;


-- ─── Dimension / reference views ─────────────────────────────────────────────

-- symbol_reference: unique symbols with metadata (populated from dim tables)
CREATE OR REPLACE VIEW symbol_reference AS
SELECT DISTINCT
    symbol          AS ticker,
    symbol          AS company_name,   -- placeholder — extend with actual master data
    'Unknown'       AS sector
FROM iceberg_catalog.silver.trades;


-- market_daily_overview: market-wide aggregates per day
CREATE OR REPLACE VIEW market_daily_overview AS
SELECT
    trade_date,
    COUNT(DISTINCT symbol)          AS symbols_traded,
    SUM(total_volume)               AS total_volume,
    SUM(total_volume * vwap)        AS total_value_traded,
    SUM(trade_count)                AS total_trades,
    AVG(avg_bid_ask_spread)         AS avg_spread,
    COUNT(CASE WHEN change_pct > 0 THEN 1 END) AS advancing,
    COUNT(CASE WHEN change_pct < 0 THEN 1 END) AS declining,
    COUNT(CASE WHEN change_pct = 0 THEN 1 END) AS unchanged
FROM daily_trade_summary
GROUP BY trade_date;


-- trader_performance_metrics: per-trader aggregates (from silver trades)
CREATE OR REPLACE VIEW trader_performance_metrics AS
SELECT
    trade_date,
    COUNT(*)                         AS total_trades,
    SUM(quantity)                    AS total_volume,
    SUM(price * quantity)            AS total_value,
    COUNT(DISTINCT symbol)           AS unique_symbols,
    COUNT(CASE WHEN side = 'BUY' THEN 1 END) AS buy_count,
    COUNT(CASE WHEN side = 'SELL' THEN 1 END) AS sell_count
FROM iceberg_catalog.silver.trades
GROUP BY trade_date;


-- sector_daily_summary: sector-level aggregates (requires symbol_reference join)
CREATE OR REPLACE VIEW sector_daily_summary AS
SELECT
    d.trade_date,
    s.sector,
    SUM(d.total_volume)                          AS total_volume,
    AVG(d.change_pct)                            AS avg_change_pct,
    SUM(d.total_volume * d.vwap)                 AS total_value
FROM daily_trade_summary d
JOIN symbol_reference s ON d.symbol = s.ticker
GROUP BY d.trade_date, s.sector;
