-- account_filter_view.sql
-- Row-Level Security: restrict silver.trades rows to the requesting account.
--
-- In the GraphQL API the current user's account_id is passed as a DuckDB
-- parameter ($account_id) before executing any query against this view.
--
-- Usage (from Python / DuckDB):
--   conn.execute("SET account_id = ?", [current_user.account_id])
--   conn.execute("SELECT * FROM silver_trades_rls_account")

CREATE OR REPLACE VIEW silver_trades_rls_account AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    trade_date,
    account_id
FROM silver.trades
WHERE account_id = current_setting('account_id');


-- Bronze equivalent (for data engineers with direct bronze access)
CREATE OR REPLACE VIEW raw_trades_rls_account AS
SELECT
    trade_id,
    symbol,
    price,
    quantity,
    side,
    timestamp,
    account_id
FROM bronze.raw_trades
WHERE account_id = current_setting('account_id');
