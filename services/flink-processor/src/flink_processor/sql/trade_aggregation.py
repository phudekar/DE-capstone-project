"""Tumbling window aggregation SQL statements for 1m, 5m, 15m windows.

Generates Flink SQL INSERT statements that compute OHLCV-style aggregates
(trade_count, total_volume, vwap, high_price, low_price) over tumbling
event-time windows. Each window size produces a separate INSERT into the
same trade_aggregates sink table, differentiated by the window_size column.

The SQL-based approach (vs. DataStream API) is used here because Flink SQL
natively supports tumbling windows with event-time watermarks and produces
efficient query plans with built-in state management and exactly-once
semantics for Kafka sinks.
"""


def trade_aggregation_insert(window_size: str, interval_expr: str) -> str:
    """Generate INSERT INTO statement for a given tumbling window size.

    Args:
        window_size: Label for the window (e.g. '1m', '5m', '15m').
        interval_expr: Flink SQL interval (e.g. "INTERVAL '1' MINUTE").
    """
    return f"""
INSERT INTO trade_aggregates
SELECT
    symbol,
    TUMBLE_START(event_time, {interval_expr}) AS window_start,
    TUMBLE_END(event_time, {interval_expr}) AS window_end,
    '{window_size}' AS window_size,
    COUNT(*) AS trade_count,
    CAST(SUM(quantity) AS BIGINT) AS total_volume,
    SUM(price * quantity) / SUM(quantity) AS vwap,
    MAX(price) AS high_price,
    MIN(price) AS low_price
FROM raw_trades
GROUP BY symbol, TUMBLE(event_time, {interval_expr})
"""


# Window configurations are defined here (not in config.py) because they are
# tightly coupled to the Flink SQL INTERVAL syntax and only used by the SQL
# pipeline. Each tuple is (label, Flink SQL interval expression).
WINDOW_CONFIGS = [
    ("1m", "INTERVAL '1' MINUTE"),
    ("5m", "INTERVAL '5' MINUTE"),
    ("15m", "INTERVAL '15' MINUTE"),
]


def all_aggregation_inserts() -> list[str]:
    """Return all tumbling window INSERT statements."""
    return [trade_aggregation_insert(ws, ie) for ws, ie in WINDOW_CONFIGS]


def enrichment_insert(lookup_exprs: dict[str, str]) -> str:
    """INSERT statement enriching raw trades with reference data via CASE expressions.

    Args:
        lookup_exprs: dict with keys 'company_name', 'sector', 'market_cap_category',
                      each containing a CASE expression for the lookup.
    """
    return f"""
INSERT INTO enriched_trades
SELECT
    event_type,
    event_id,
    `timestamp`,
    trade_id,
    symbol,
    price,
    quantity,
    buy_order_id,
    sell_order_id,
    buyer_agent_id,
    seller_agent_id,
    is_aggressive_buy,
    {lookup_exprs["company_name"]} AS company_name,
    {lookup_exprs["sector"]} AS sector,
    {lookup_exprs["market_cap_category"]} AS market_cap_category
FROM raw_trades
"""
