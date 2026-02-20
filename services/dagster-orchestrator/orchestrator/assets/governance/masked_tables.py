"""masked_tables.py
Dagster assets that materialise role-specific masked views of the Iceberg tables.

These assets run after the silver and gold layers are materialised and apply the
DuckDB masking macros to produce pre-computed masked copies consumed by the
GraphQL API and BI tools.

Assets:
  masked_silver_trades_analyst    – silver trades with account_id hashed
  masked_silver_trades_business   – silver trades with account_id redacted + price rounded
  masked_gold_summary_public      – gold daily summary (no PII, just CLS schema)
"""

import logging

from dagster import AssetExecutionContext, AssetKey, asset

logger = logging.getLogger(__name__)

# SQL for each masked view (executed in DuckDB against Iceberg tables)

_ANALYST_MASK_SQL = """
CREATE OR REPLACE VIEW silver_trades_analyst AS
SELECT
    trade_id,
    symbol,
    ROUND(price, 2)               AS price,
    quantity,
    side,
    trade_date,
    md5(CAST(account_id AS VARCHAR)) AS account_id
FROM silver.trades;
"""

_BUSINESS_MASK_SQL = """
CREATE OR REPLACE VIEW silver_trades_business AS
SELECT
    trade_id,
    symbol,
    ROUND(price, 0)               AS price,
    quantity,
    side,
    trade_date,
    '***REDACTED***'              AS account_id
FROM silver.trades;
"""

_GOLD_PUBLIC_SQL = """
CREATE OR REPLACE VIEW gold_summary_public AS
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
"""


@asset(
    key=AssetKey("masked_silver_trades_analyst"),
    group_name="governance",
    deps=[AssetKey("silver_trades")],
    description="Analyst-scoped masked view of silver trades: account_id hashed, price rounded to 2dp.",
    kinds={"duckdb"},
)
def masked_silver_trades_analyst(
    context: AssetExecutionContext,
) -> None:
    """Create analyst masked view of silver.trades."""
    context.log.info("Creating masked_silver_trades_analyst view...")
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(_ANALYST_MASK_SQL)
        context.log.info("masked_silver_trades_analyst view created successfully.")
    except Exception as exc:
        context.log.warning(
            "Could not create masked analyst view (Iceberg may not be running): %s", exc
        )


@asset(
    key=AssetKey("masked_silver_trades_business"),
    group_name="governance",
    deps=[AssetKey("silver_trades")],
    description="Business-user masked view of silver trades: account_id redacted, price rounded.",
    kinds={"duckdb"},
)
def masked_silver_trades_business(
    context: AssetExecutionContext,
) -> None:
    """Create business-user masked view of silver.trades."""
    context.log.info("Creating masked_silver_trades_business view...")
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(_BUSINESS_MASK_SQL)
        context.log.info("masked_silver_trades_business view created successfully.")
    except Exception as exc:
        context.log.warning(
            "Could not create masked business view (Iceberg may not be running): %s", exc
        )


@asset(
    key=AssetKey("masked_gold_summary_public"),
    group_name="governance",
    deps=[AssetKey("gold_daily_trading_summary")],
    description="Public CLS-scoped view of gold daily summary: explicit column list, no PII.",
    kinds={"duckdb"},
)
def masked_gold_summary_public(
    context: AssetExecutionContext,
) -> None:
    """Create public CLS view of gold.daily_trading_summary."""
    context.log.info("Creating masked_gold_summary_public view...")
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(_GOLD_PUBLIC_SQL)
        context.log.info("masked_gold_summary_public view created successfully.")
    except Exception as exc:
        context.log.warning(
            "Could not create gold public view (Iceberg may not be running): %s", exc
        )
