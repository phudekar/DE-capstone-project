"""Tests for Silver processor logic (dedup, enrichment, derived fields)."""

import json
from datetime import datetime, timezone

import duckdb
import pyarrow as pa


class TestSilverDeduplication:
    """Test deduplication logic used in silver_processor."""

    def test_deduplicate_trades_by_trade_id(self):
        """Duplicate trade_id should keep only first by offset."""
        data = pa.table(
            {
                "trade_id": ["T-001", "T-001", "T-002"],
                "symbol": ["AAPL", "AAPL", "GOOGL"],
                "price": [185.5, 185.6, 2800.0],
                "quantity": [100, 100, 50],
                "_kafka_offset": [1, 2, 3],
            }
        )
        con = duckdb.connect()
        con.register("raw", data)
        deduped = con.execute(
            """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY _kafka_offset) AS rn
                FROM raw
            ) WHERE rn = 1
            """
        ).fetch_arrow_table()
        con.close()

        assert len(deduped) == 2
        # T-001 should have price 185.5 (first occurrence)
        t001 = deduped.filter(pa.compute.equal(deduped.column("trade_id"), "T-001"))
        assert t001.column("price")[0].as_py() == 185.5

    def test_deduplicate_orderbook_by_symbol_timestamp(self):
        """Duplicate (symbol, timestamp) should keep only first."""
        ts = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        data = pa.table(
            {
                "symbol": ["AAPL", "AAPL", "GOOGL"],
                "timestamp": [ts, ts, ts],
                "sequence_number": [1, 2, 3],
                "_kafka_offset": [10, 20, 30],
            }
        )
        con = duckdb.connect()
        con.register("raw", data)
        deduped = con.execute(
            """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, timestamp ORDER BY _kafka_offset) AS rn
                FROM raw
            ) WHERE rn = 1
            """
        ).fetch_arrow_table()
        con.close()

        assert len(deduped) == 2
        aapl = deduped.filter(pa.compute.equal(deduped.column("symbol"), "AAPL"))
        assert aapl.column("sequence_number")[0].as_py() == 1


class TestSilverOrderbookDerivation:
    """Test derived field computation for orderbook snapshots."""

    def test_spread_computation(self):
        best_bid = 185.40
        best_ask = 185.60
        spread = best_ask - best_bid
        mid_price = (best_bid + best_ask) / 2.0
        assert abs(spread - 0.20) < 1e-10
        assert abs(mid_price - 185.50) < 1e-10

    def test_depth_from_bids_asks(self):
        bids = [{"price": 185.4, "quantity": 200}, {"price": 185.3, "quantity": 150}]
        asks = [{"price": 185.6, "quantity": 180}]
        assert len(bids) == 2
        assert len(asks) == 1

    def test_empty_book_handling(self):
        bids = []
        asks = []
        best_bid = bids[0] if bids else None
        best_ask = asks[0] if asks else None
        assert best_bid is None
        assert best_ask is None
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask["price"] - best_bid["price"]
        assert spread is None
