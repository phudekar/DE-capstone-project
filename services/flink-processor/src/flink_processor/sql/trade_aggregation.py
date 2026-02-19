"""Tumbling window aggregation SQL statements for 1m, 5m, 15m windows."""


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


WINDOW_CONFIGS = [
    ("1m", "INTERVAL '1' MINUTE"),
    ("5m", "INTERVAL '5' MINUTE"),
    ("15m", "INTERVAL '15' MINUTE"),
]


def all_aggregation_inserts() -> list[str]:
    """Return all tumbling window INSERT statements."""
    return [trade_aggregation_insert(ws, ie) for ws, ie in WINDOW_CONFIGS]


def enrichment_insert() -> str:
    """INSERT statement joining raw trades with reference symbols."""
    return """
INSERT INTO enriched_trades
SELECT
    t.event_type,
    t.`timestamp`,
    t.trade_id,
    t.symbol,
    t.price,
    t.quantity,
    t.buyer_order_id,
    t.seller_order_id,
    t.buyer_agent_id,
    t.seller_agent_id,
    t.aggressor_side,
    r.company_name,
    r.sector,
    r.market_cap_category
FROM raw_trades AS t
LEFT JOIN reference_symbols AS r
    ON t.symbol = r.symbol
"""
