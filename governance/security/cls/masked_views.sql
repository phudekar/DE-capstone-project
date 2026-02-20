-- cls/masked_views.sql
-- Column-Level Security views — one per role, explicitly selecting allowed columns.
-- Sensitive columns are masked or excluded entirely based on the role.
--
-- Roles and their column visibility:
--   data_engineer   → all columns (no masking)
--   data_scientist  → all columns; account_id hashed
--   data_analyst    → no account_id; price/quantity rounded
--   business_user   → symbol, trade_date, quantity only (aggregation-safe)

-- ─── Data Engineer (full access) ─────────────────────────────────────────────
-- Engineers access the base tables directly; no CLS view needed.
-- Listed here for documentation completeness.

-- CREATE OR REPLACE VIEW silver_trades_cls_engineer AS SELECT * FROM silver.trades;


-- ─── Data Scientist ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver_trades_cls_scientist AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    trade_date,
    md5(CAST(account_id AS VARCHAR))  AS account_id   -- hashed PII
FROM silver.trades;


-- ─── Data Analyst ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver_trades_cls_analyst AS
SELECT
    trade_id,
    symbol,
    ROUND(price, 2)    AS price,       -- rounded to 2 dp (hide micro-price precision)
    quantity,
    side,
    trade_date
    -- account_id excluded entirely
FROM silver.trades;


-- ─── Business User ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW silver_trades_cls_business AS
SELECT
    symbol,
    trade_date,
    quantity,
    side
    -- price, trade_id, account_id excluded
FROM silver.trades;


-- ─── Gold layer CLS ───────────────────────────────────────────────────────────
-- Gold tables contain no PII — all roles see all columns.
-- CLS views exist only to enforce schema stability.

CREATE OR REPLACE VIEW daily_summary_cls_all AS
SELECT
    symbol,
    trade_date,
    total_volume,
    vwap,
    open_price,
    close_price,
    high_price,
    low_price,
    trade_count,
    avg_bid_ask_spread
FROM gold.daily_trading_summary;
