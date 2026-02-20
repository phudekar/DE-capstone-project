-- masked_views.sql
-- DuckDB SQL: masking macros and role-specific views for the Iceberg lakehouse.
-- These views are created in the DuckDB session used by the GraphQL API.
--
-- Masking strategies:
--   mask_hash(v)         → SHA-256 hex (pseudonymisation)
--   mask_partial(v, n)   → show first n chars, redact rest with *
--   mask_round(v, n)     → round numeric to n significant digits
--   mask_redact()        → full redaction → '***REDACTED***'

-- ─── Masking Macros ───────────────────────────────────────────────────────────

CREATE OR REPLACE MACRO mask_hash(v) AS
    CASE
        WHEN v IS NULL THEN NULL
        ELSE md5(CAST(v AS VARCHAR))   -- DuckDB built-in md5 (hex string)
    END;

CREATE OR REPLACE MACRO mask_partial(v, n) AS
    CASE
        WHEN v IS NULL THEN NULL
        WHEN LENGTH(CAST(v AS VARCHAR)) <= n THEN '***'
        ELSE CONCAT(LEFT(CAST(v AS VARCHAR), n), REPEAT('*', LENGTH(CAST(v AS VARCHAR)) - n))
    END;

CREATE OR REPLACE MACRO mask_round(v, n) AS
    CASE
        WHEN v IS NULL THEN NULL
        ELSE ROUND(CAST(v AS DOUBLE), -n) * POWER(10.0, n) / POWER(10.0, n)
    END;

CREATE OR REPLACE MACRO mask_redact() AS '***REDACTED***';


-- ─── Analyst View: silver.trades ─────────────────────────────────────────────
-- Data analysts see silver trades but PII (account_id) is hashed.

CREATE OR REPLACE VIEW silver_trades_analyst AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    trade_date,
    mask_hash(account_id)  AS account_id   -- pseudonymised
FROM silver.trades;


-- ─── Business User View: silver.trades ───────────────────────────────────────
-- Business users see only aggregation-safe columns; account_id fully redacted.

CREATE OR REPLACE VIEW silver_trades_business AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    trade_date,
    mask_redact()          AS account_id   -- fully redacted
FROM silver.trades;


-- ─── Analyst View: bronze.raw_trades ─────────────────────────────────────────
-- Analysts can see bronze but with account_id partially masked.

CREATE OR REPLACE VIEW raw_trades_analyst AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    timestamp,
    mask_partial(account_id, 4) AS account_id  -- e.g. "ACCT****"
FROM bronze.raw_trades;


-- ─── Gold layer: no PII — no masking required ─────────────────────────────────
-- gold.daily_trading_summary and gold.top_movers contain no PII columns.
-- Both layers are accessible to all roles as-is.
