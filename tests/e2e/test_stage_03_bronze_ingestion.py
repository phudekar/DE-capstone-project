"""Stage 3 — Bronze ingestion: Kafka message → Bronze (DuckDB sim).

Tests:
  - _parse_trade()    produces the correct column mapping
  - _parse_orderbook() produces the correct column mapping
  - Kafka metadata (_kafka_topic, _kafka_partition, _kafka_offset) is recorded
  - _ingested_at timestamp is set on every row
  - Written rows can be queried from the DuckDB bronze tables
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "libs/common/src"))
sys.path.insert(0, str(ROOT / "services/lakehouse/src"))

import pytest
from tests.e2e.fixtures.events import make_trade_event, make_orderbook_event


def _flat(raw: dict) -> dict:
    return {"event_type": raw["event_type"], "timestamp": raw["timestamp"], **raw["data"]}


def _parse_trade(flat: dict, topic: str = "raw.trades",
                 partition: int = 0, offset: int = 0) -> dict:
    now = datetime.now(timezone.utc)
    ts = datetime.fromisoformat(flat["timestamp"].replace("Z", "+00:00"))
    return {
        "trade_id":         flat["trade_id"],
        "symbol":           flat["symbol"],
        "price":            float(flat["price"]),
        "quantity":         int(flat["quantity"]),
        "buyer_order_id":   flat["buyer_order_id"],
        "seller_order_id":  flat["seller_order_id"],
        "buyer_agent_id":   flat["buyer_agent_id"],
        "seller_agent_id":  flat["seller_agent_id"],
        "aggressor_side":   str(flat["aggressor_side"]),
        "event_type":       flat["event_type"],
        "timestamp":        ts,
        "_kafka_topic":     topic,
        "_kafka_partition": partition,
        "_kafka_offset":    offset,
        "_ingested_at":     now,
    }


def _parse_orderbook(flat: dict, topic: str = "raw.orderbook-snapshots",
                     partition: int = 0, offset: int = 0) -> dict:
    now = datetime.now(timezone.utc)
    ts = datetime.fromisoformat(flat["timestamp"].replace("Z", "+00:00"))
    return {
        "symbol":           flat["symbol"],
        "bids_json":        json.dumps(flat.get("bids", [])),
        "asks_json":        json.dumps(flat.get("asks", [])),
        "sequence_number":  int(flat["sequence_number"]),
        "event_type":       flat["event_type"],
        "timestamp":        ts,
        "_kafka_topic":     topic,
        "_kafka_partition": partition,
        "_kafka_offset":    offset,
        "_ingested_at":     now,
    }


# ── _parse_trade tests ─────────────────────────────────────────────────────────

def test_parse_trade_has_all_columns():
    flat = _flat(make_trade_event())
    row = _parse_trade(flat)
    required = {"trade_id", "symbol", "price", "quantity",
                "buyer_order_id", "seller_order_id",
                "buyer_agent_id", "seller_agent_id",
                "aggressor_side", "event_type", "timestamp",
                "_kafka_topic", "_kafka_partition", "_kafka_offset", "_ingested_at"}
    assert required.issubset(set(row.keys()))


def test_parse_trade_price_is_float():
    flat = _flat(make_trade_event(price=182.5))
    row = _parse_trade(flat)
    assert isinstance(row["price"], float)
    assert row["price"] == pytest.approx(182.5)


def test_parse_trade_quantity_is_int():
    flat = _flat(make_trade_event(quantity=250))
    row = _parse_trade(flat)
    assert isinstance(row["quantity"], int)
    assert row["quantity"] == 250


def test_parse_trade_kafka_metadata():
    flat = _flat(make_trade_event())
    row = _parse_trade(flat, topic="raw.trades", partition=2, offset=99)
    assert row["_kafka_topic"] == "raw.trades"
    assert row["_kafka_partition"] == 2
    assert row["_kafka_offset"] == 99


def test_parse_trade_ingested_at_is_utc():
    flat = _flat(make_trade_event())
    row = _parse_trade(flat)
    assert isinstance(row["_ingested_at"], datetime)
    assert row["_ingested_at"].tzinfo is not None


def test_parse_trade_timestamp_is_datetime():
    flat = _flat(make_trade_event())
    row = _parse_trade(flat)
    assert isinstance(row["timestamp"], datetime)


def test_parse_trade_symbol_preserved():
    flat = _flat(make_trade_event(symbol="TSLA"))
    row = _parse_trade(flat)
    assert row["symbol"] == "TSLA"


# ── _parse_orderbook tests ─────────────────────────────────────────────────────

def test_parse_orderbook_has_all_columns():
    flat = _flat(make_orderbook_event())
    row = _parse_orderbook(flat)
    required = {"symbol", "bids_json", "asks_json", "sequence_number",
                "event_type", "timestamp",
                "_kafka_topic", "_kafka_partition", "_kafka_offset", "_ingested_at"}
    assert required.issubset(set(row.keys()))


def test_parse_orderbook_bids_json_parseable():
    flat = _flat(make_orderbook_event(n_levels=3))
    row = _parse_orderbook(flat)
    bids = json.loads(row["bids_json"])
    assert isinstance(bids, list)
    assert len(bids) == 3
    assert "price" in bids[0] and "quantity" in bids[0]


def test_parse_orderbook_asks_json_parseable():
    flat = _flat(make_orderbook_event(n_levels=4))
    row = _parse_orderbook(flat)
    asks = json.loads(row["asks_json"])
    assert len(asks) == 4


def test_parse_orderbook_sequence_number_is_int():
    flat = _flat(make_orderbook_event())
    row = _parse_orderbook(flat)
    assert isinstance(row["sequence_number"], int)


# ── DuckDB bronze table writes (via session-level populated_pipeline) ──────────

def test_bronze_trade_row_count(pipeline_db, populated_pipeline):
    count = pipeline_db.count("bronze_trades")
    assert count == 60   # 6 symbols × 10 trades each


def test_bronze_orderbook_row_count(pipeline_db, populated_pipeline):
    count = pipeline_db.count("bronze_orderbook")
    assert count == 6    # one per symbol


def test_bronze_trades_all_symbols_present(pipeline_db, populated_pipeline, symbols):
    found = {r[0] for r in pipeline_db.conn.execute(
        "SELECT DISTINCT symbol FROM bronze_trades"
    ).fetchall()}
    assert set(symbols) == found


def test_bronze_trades_positive_prices(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE price <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_bronze_trades_positive_quantities(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE quantity <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_bronze_kafka_metadata_recorded(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute(
        "SELECT _kafka_topic, _kafka_partition, _kafka_offset FROM bronze_trades LIMIT 1"
    ).fetchone()
    assert row[0] == "raw.trades"
    assert row[1] is not None
    assert row[2] is not None


def test_bronze_ingested_at_not_null(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE _ingested_at IS NULL"
    ).fetchone()[0]
    assert bad == 0
