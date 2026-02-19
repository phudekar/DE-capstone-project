"""Tests for Bronze writer message parsing logic."""

import json
from datetime import datetime, timezone

from lakehouse.writers.bronze_writer import BronzeWriter


class TestBronzeWriterParsing:
    """Test the parsing functions without Kafka/Iceberg dependencies."""

    def setup_method(self):
        # Create writer without initializing Kafka/Iceberg connections
        self.writer = BronzeWriter.__new__(BronzeWriter)

    def test_parse_trade(self, sample_trade_message):
        row = self.writer._parse_trade(sample_trade_message, "raw.trades", 0, 42)

        assert row["trade_id"] == "T-001"
        assert row["symbol"] == "AAPL"
        assert row["price"] == 185.50
        assert row["quantity"] == 100
        assert row["buyer_order_id"] == "BO-001"
        assert row["seller_order_id"] == "SO-001"
        assert row["aggressor_side"] == "Buy"
        assert row["event_type"] == "TradeExecuted"
        assert row["_kafka_topic"] == "raw.trades"
        assert row["_kafka_partition"] == 0
        assert row["_kafka_offset"] == 42
        assert isinstance(row["timestamp"], datetime)
        assert isinstance(row["_ingested_at"], datetime)

    def test_parse_orderbook(self, sample_orderbook_message):
        row = self.writer._parse_orderbook(
            sample_orderbook_message, "raw.orderbook-snapshots", 1, 99
        )

        assert row["symbol"] == "AAPL"
        assert row["sequence_number"] == 42
        assert row["event_type"] == "OrderBookSnapshot"
        assert row["_kafka_topic"] == "raw.orderbook-snapshots"
        assert row["_kafka_partition"] == 1
        assert row["_kafka_offset"] == 99

        # Verify bids/asks are JSON strings
        bids = json.loads(row["bids_json"])
        asks = json.loads(row["asks_json"])
        assert len(bids) == 2
        assert len(asks) == 2
        assert bids[0]["price"] == 185.40
        assert asks[0]["price"] == 185.60

    def test_parse_trade_timestamp(self, sample_trade_message):
        row = self.writer._parse_trade(sample_trade_message, "raw.trades", 0, 0)
        assert row["timestamp"].year == 2024
        assert row["timestamp"].month == 1
        assert row["timestamp"].day == 15
        assert row["timestamp"].hour == 10
        assert row["timestamp"].minute == 30
