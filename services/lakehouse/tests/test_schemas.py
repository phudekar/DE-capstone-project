"""Tests for Iceberg schema definitions."""

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestamptzType

from lakehouse.schemas.bronze import RAW_ORDERBOOK_SCHEMA, RAW_TRADES_SCHEMA
from lakehouse.schemas.dimensions import DIM_SYMBOL_SCHEMA, DIM_TIME_SCHEMA
from lakehouse.schemas.gold import DAILY_TRADING_SUMMARY_SCHEMA
from lakehouse.schemas.silver import ORDERBOOK_SNAPSHOTS_SCHEMA, TRADES_SCHEMA


class TestBronzeSchemas:
    def test_raw_trades_has_kafka_metadata(self):
        field_names = {f.name for f in RAW_TRADES_SCHEMA.fields}
        assert "_kafka_topic" in field_names
        assert "_kafka_partition" in field_names
        assert "_kafka_offset" in field_names
        assert "_ingested_at" in field_names

    def test_raw_trades_has_trade_fields(self):
        field_names = {f.name for f in RAW_TRADES_SCHEMA.fields}
        assert "trade_id" in field_names
        assert "symbol" in field_names
        assert "price" in field_names
        assert "quantity" in field_names
        assert "aggressor_side" in field_names

    def test_raw_orderbook_has_bids_asks_json(self):
        field_names = {f.name for f in RAW_ORDERBOOK_SCHEMA.fields}
        assert "bids_json" in field_names
        assert "asks_json" in field_names
        assert "sequence_number" in field_names

    def test_raw_orderbook_has_kafka_metadata(self):
        field_names = {f.name for f in RAW_ORDERBOOK_SCHEMA.fields}
        assert "_kafka_topic" in field_names
        assert "_ingested_at" in field_names


class TestSilverSchemas:
    def test_trades_has_enrichment_fields(self):
        field_names = {f.name for f in TRADES_SCHEMA.fields}
        assert "company_name" in field_names
        assert "sector" in field_names
        assert "_processed_at" in field_names

    def test_trades_no_kafka_metadata(self):
        field_names = {f.name for f in TRADES_SCHEMA.fields}
        assert "_kafka_topic" not in field_names
        assert "_kafka_offset" not in field_names

    def test_orderbook_has_derived_fields(self):
        field_names = {f.name for f in ORDERBOOK_SNAPSHOTS_SCHEMA.fields}
        assert "spread" in field_names
        assert "mid_price" in field_names
        assert "best_bid_price" in field_names
        assert "best_ask_price" in field_names
        assert "bid_depth" in field_names
        assert "ask_depth" in field_names


class TestGoldSchemas:
    def test_daily_summary_has_ohlcv(self):
        field_names = {f.name for f in DAILY_TRADING_SUMMARY_SCHEMA.fields}
        assert "open_price" in field_names
        assert "close_price" in field_names
        assert "high_price" in field_names
        assert "low_price" in field_names
        assert "vwap" in field_names
        assert "total_volume" in field_names
        assert "trade_count" in field_names

    def test_daily_summary_has_aggregation_metadata(self):
        field_names = {f.name for f in DAILY_TRADING_SUMMARY_SCHEMA.fields}
        assert "_aggregated_at" in field_names
        assert "trading_date" in field_names


class TestDimensionSchemas:
    def test_dim_symbol_has_scd2_fields(self):
        field_names = {f.name for f in DIM_SYMBOL_SCHEMA.fields}
        assert "effective_date" in field_names
        assert "expiry_date" in field_names
        assert "is_current" in field_names
        assert "row_hash" in field_names
        assert "symbol_key" in field_names

    def test_dim_time_has_session_fields(self):
        field_names = {f.name for f in DIM_TIME_SCHEMA.fields}
        assert "time_key" in field_names
        assert "hour" in field_names
        assert "minute" in field_names
        assert "trading_session" in field_names
        assert "is_market_hours" in field_names
