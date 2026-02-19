"""Kafka source/sink CREATE TABLE DDL for Flink SQL."""

from flink_processor.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ENRICHED_TRADES,
    TOPIC_RAW_TRADES,
    TOPIC_TRADE_AGGREGATES,
)


def raw_trades_source_ddl() -> str:
    """Kafka source table for raw trade events (JSON)."""
    return f"""
CREATE TABLE raw_trades (
    event_type STRING,
    `timestamp` STRING,
    trade_id STRING,
    symbol STRING,
    price DOUBLE,
    quantity INT,
    buyer_order_id STRING,
    seller_order_id STRING,
    buyer_agent_id STRING,
    seller_agent_id STRING,
    aggressor_side STRING,
    event_time AS TO_TIMESTAMP(`timestamp`),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{TOPIC_RAW_TRADES}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-sql-trade-aggregation',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
"""


def trade_aggregates_sink_ddl() -> str:
    """Kafka sink table for windowed trade aggregates."""
    return f"""
CREATE TABLE trade_aggregates (
    symbol STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    window_size STRING,
    trade_count BIGINT,
    total_volume BIGINT,
    vwap DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = '{TOPIC_TRADE_AGGREGATES}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
"""


def reference_symbols_ddl(symbols_data: list[dict]) -> str:
    """In-memory reference table for symbol enrichment via datagen + VALUES.

    Uses a TEMPORARY VIEW created from hardcoded VALUES for the lookup join.
    """
    rows = []
    for s in symbols_data:
        rows.append(
            f"('{s['symbol']}', '{s['company_name']}', '{s['sector']}', '{s['market_cap_category']}')"
        )
    values_csv = ",\n        ".join(rows)
    return f"""
CREATE TEMPORARY VIEW reference_symbols AS
SELECT * FROM (
    VALUES
        {values_csv}
) AS t(symbol, company_name, sector, market_cap_category)
"""


def enriched_trades_sink_ddl() -> str:
    """Kafka sink table for enriched trades."""
    return f"""
CREATE TABLE enriched_trades (
    event_type STRING,
    `timestamp` STRING,
    trade_id STRING,
    symbol STRING,
    price DOUBLE,
    quantity INT,
    buyer_order_id STRING,
    seller_order_id STRING,
    buyer_agent_id STRING,
    seller_agent_id STRING,
    aggressor_side STRING,
    company_name STRING,
    sector STRING,
    market_cap_category STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{TOPIC_ENRICHED_TRADES}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
)
"""
