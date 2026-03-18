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
    event_id STRING,
    `timestamp` STRING,
    trade_id STRING,
    symbol STRING,
    price DOUBLE,
    quantity INT,
    buy_order_id STRING,
    sell_order_id STRING,
    buyer_agent_id STRING,
    seller_agent_id STRING,
    is_aggressive_buy BOOLEAN,
    event_time AS TO_TIMESTAMP(SUBSTRING(`timestamp`, 1, 23), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
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


def reference_symbols_ddl(symbols_data: list[dict]) -> dict[str, str]:
    """Build a CASE-expression UDF-style enrichment map from reference data.

    Instead of a JOIN (which produces update streams incompatible with
    Kafka append-only sinks), we generate CASE expressions that map
    symbol → company_name/sector/market_cap_category inline.
    """
    company_cases = []
    sector_cases = []
    cap_cases = []
    for s in symbols_data:
        sym = s["symbol"]
        company = s["company_name"].replace("'", "''")
        sector = s["sector"].replace("'", "''")
        cap = s["market_cap_category"]
        company_cases.append(f"WHEN '{sym}' THEN '{company}'")
        sector_cases.append(f"WHEN '{sym}' THEN '{sector}'")
        cap_cases.append(f"WHEN '{sym}' THEN '{cap}'")

    return {
        "company_name": "CASE symbol " + " ".join(company_cases) + " ELSE 'Unknown' END",
        "sector": "CASE symbol " + " ".join(sector_cases) + " ELSE 'Unknown' END",
        "market_cap_category": "CASE symbol " + " ".join(cap_cases) + " ELSE 'Unknown' END",
    }


def enriched_trades_sink_ddl() -> str:
    """Kafka sink table for enriched trades."""
    return f"""
CREATE TABLE enriched_trades (
    event_type STRING,
    event_id STRING,
    `timestamp` STRING,
    trade_id STRING,
    symbol STRING,
    price DOUBLE,
    quantity INT,
    buy_order_id STRING,
    sell_order_id STRING,
    buyer_agent_id STRING,
    seller_agent_id STRING,
    is_aggressive_buy BOOLEAN,
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
