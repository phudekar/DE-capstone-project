"""Shared fixtures for the end-to-end pipeline test suite.

Architecture simulated (no live Docker services required):

  DE-Stock WebSocket events
        │  (JSON dicts)
        ▼
  Kafka Bridge  (validate → route → flatten)
        │  (flat JSON dicts, keyed by topic)
        ▼
  Bronze layer  (in-memory DuckDB tables)
        │
        ▼
  Silver layer  (dedup + enrich via Python logic on DuckDB)
        │
        ▼
  Gold layer    (OHLCV aggregation on DuckDB)
        │
        ▼
  GraphQL API   (resolvers query the DuckDB tables)
        │
        ▼
  Superset      (saved-query SQL runs against DuckDB)

All service code is imported directly via sys.path manipulation —
no Kafka brokers, Iceberg REST catalogs, or Docker containers needed.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

# ── Path helpers ──────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent.parent.parent

def _add(rel: str) -> None:
    p = str(ROOT / rel)
    if p not in sys.path:
        sys.path.insert(0, p)

_add("libs/common/src")
_add("services/kafka-bridge/src")
_add("services/lakehouse/src")
_add("services/graphql-api")
_add("services/superset/bootstrap")


# ── Shared test constants ─────────────────────────────────────────────────────

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM"]

SYMBOL_DIM = {
    "AAPL": {"company_name": "Apple Inc.",         "sector": "Technology"},
    "MSFT": {"company_name": "Microsoft Corp.",    "sector": "Technology"},
    "GOOG": {"company_name": "Alphabet Inc.",      "sector": "Technology"},
    "AMZN": {"company_name": "Amazon.com Inc.",    "sector": "Consumer Discretionary"},
    "TSLA": {"company_name": "Tesla Inc.",         "sector": "Consumer Discretionary"},
    "JPM":  {"company_name": "JPMorgan Chase",     "sector": "Financials"},
}


# ─────────────────────────────────────────────────────────────────────────────
# PipelineDB — in-memory DuckDB with Bronze / Silver / Gold tables
# ─────────────────────────────────────────────────────────────────────────────

class PipelineDB:
    """In-memory DuckDB that mirrors the full lakehouse medallion schema."""

    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")
        self._create_tables()

    def _create_tables(self) -> None:
        self.conn.execute("""
            CREATE TABLE bronze_trades (
                trade_id        VARCHAR NOT NULL,
                symbol          VARCHAR NOT NULL,
                price           DOUBLE  NOT NULL,
                quantity        INTEGER NOT NULL,
                buyer_order_id  VARCHAR NOT NULL,
                seller_order_id VARCHAR NOT NULL,
                buyer_agent_id  VARCHAR NOT NULL,
                seller_agent_id VARCHAR NOT NULL,
                aggressor_side  VARCHAR NOT NULL,
                event_type      VARCHAR NOT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                _kafka_topic    VARCHAR NOT NULL,
                _kafka_partition INTEGER NOT NULL,
                _kafka_offset   BIGINT  NOT NULL,
                _ingested_at    TIMESTAMPTZ NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE bronze_orderbook (
                symbol          VARCHAR NOT NULL,
                bids_json       VARCHAR NOT NULL,
                asks_json       VARCHAR NOT NULL,
                sequence_number INTEGER NOT NULL,
                event_type      VARCHAR NOT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                _kafka_topic    VARCHAR NOT NULL,
                _kafka_partition INTEGER NOT NULL,
                _kafka_offset   BIGINT  NOT NULL,
                _ingested_at    TIMESTAMPTZ NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE silver_trades (
                trade_id        VARCHAR NOT NULL,
                symbol          VARCHAR NOT NULL,
                price           DOUBLE  NOT NULL,
                quantity        INTEGER NOT NULL,
                buyer_order_id  VARCHAR NOT NULL,
                seller_order_id VARCHAR NOT NULL,
                buyer_agent_id  VARCHAR NOT NULL,
                seller_agent_id VARCHAR NOT NULL,
                aggressor_side  VARCHAR NOT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                company_name    VARCHAR,
                sector          VARCHAR,
                _processed_at   TIMESTAMPTZ NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE silver_orderbook (
                symbol          VARCHAR NOT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                best_bid_price  DOUBLE,
                best_bid_qty    INTEGER,
                best_ask_price  DOUBLE,
                best_ask_qty    INTEGER,
                bid_depth       INTEGER NOT NULL,
                ask_depth       INTEGER NOT NULL,
                spread          DOUBLE,
                mid_price       DOUBLE,
                sequence_number INTEGER NOT NULL,
                company_name    VARCHAR,
                sector          VARCHAR,
                _processed_at   TIMESTAMPTZ NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE gold_daily_summary (
                symbol          VARCHAR NOT NULL,
                trading_date    DATE    NOT NULL,
                open_price      DOUBLE  NOT NULL,
                close_price     DOUBLE  NOT NULL,
                high_price      DOUBLE  NOT NULL,
                low_price       DOUBLE  NOT NULL,
                vwap            DOUBLE  NOT NULL,
                total_volume    BIGINT  NOT NULL,
                trade_count     BIGINT  NOT NULL,
                total_value     DOUBLE  NOT NULL,
                company_name    VARCHAR,
                sector          VARCHAR,
                _aggregated_at  TIMESTAMPTZ NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE dim_symbol (
                symbol        VARCHAR NOT NULL PRIMARY KEY,
                company_name  VARCHAR,
                sector        VARCHAR,
                is_current    BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        self.conn.executemany(
            "INSERT INTO dim_symbol VALUES (?, ?, ?, TRUE)",
            [(s, d["company_name"], d["sector"]) for s, d in SYMBOL_DIM.items()],
        )

    def count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def close(self) -> None:
        self.conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# PipelineSimulator — runs each stage using service code + PipelineDB
# ─────────────────────────────────────────────────────────────────────────────

class PipelineSimulator:
    """Drives raw WebSocket events through every pipeline stage into DuckDB."""

    def __init__(self, db: PipelineDB) -> None:
        self.db = db
        self._kafka_offset = 0

    # ── Stage 2: Kafka Bridge ─────────────────────────────────────────────────

    def bridge_process(self, raw_event: dict) -> dict | None:
        """Validate + route a raw event (returns flat dict to write to Kafka, or None)."""
        from kafka_bridge.message_router import route_event
        from kafka_bridge.validation.message_validator import validate_message

        event = validate_message(raw_event)
        if event is None:
            return None

        event_type = raw_event["event_type"]
        data       = raw_event["data"]
        flat       = {"event_type": event_type, "timestamp": raw_event["timestamp"], **data}
        route      = route_event(event_type, data)
        return {"topic": route.topic, "key": route.key, "flat": flat}

    # ── Stage 3: Bronze Ingestion ─────────────────────────────────────────────

    def ingest_to_bronze(self, flat: dict, topic: str) -> None:
        """Write a flat Kafka message to the correct Bronze table."""
        now = datetime.now(timezone.utc)
        offset = self._kafka_offset
        self._kafka_offset += 1
        ts = datetime.fromisoformat(
            flat["timestamp"].replace("Z", "+00:00")
            if isinstance(flat["timestamp"], str)
            else flat["timestamp"].isoformat()
        )

        if topic == "raw.trades":
            self.db.conn.execute("""
                INSERT INTO bronze_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                flat["trade_id"], flat["symbol"],
                float(flat["price"]), int(flat["quantity"]),
                flat["buyer_order_id"], flat["seller_order_id"],
                flat["buyer_agent_id"], flat["seller_agent_id"],
                str(flat["aggressor_side"]),
                flat["event_type"], ts,
                topic, 0, offset, now,
            ))

        elif topic == "raw.orderbook-snapshots":
            self.db.conn.execute("""
                INSERT INTO bronze_orderbook VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                flat["symbol"],
                json.dumps(flat.get("bids", [])),
                json.dumps(flat.get("asks", [])),
                int(flat["sequence_number"]),
                flat["event_type"], ts,
                topic, 0, offset, now,
            ))

    # ── Stage 4: Silver Transformation ────────────────────────────────────────

    def process_silver_trades(self) -> int:
        """Dedup + enrich bronze_trades → silver_trades. Returns row count written."""
        conn = self.db.conn
        # Fetch unprocessed bronze rows
        bronze = conn.execute("""
            SELECT b.* FROM bronze_trades b
            LEFT JOIN silver_trades s USING (trade_id)
            WHERE s.trade_id IS NULL
        """).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM bronze_trades LIMIT 0").description]
        if not bronze:
            return 0

        now = datetime.now(timezone.utc)
        written = 0
        seen = set()
        for row in bronze:
            rd = dict(zip(cols, row))
            if rd["trade_id"] in seen:
                continue
            seen.add(rd["trade_id"])
            dim = SYMBOL_DIM.get(rd["symbol"], {})
            conn.execute("""
                INSERT INTO silver_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rd["trade_id"], rd["symbol"],
                rd["price"], rd["quantity"],
                rd["buyer_order_id"], rd["seller_order_id"],
                rd["buyer_agent_id"], rd["seller_agent_id"],
                rd["aggressor_side"], rd["timestamp"],
                dim.get("company_name"), dim.get("sector"), now,
            ))
            written += 1
        return written

    def process_silver_orderbook(self) -> int:
        """Parse + enrich bronze_orderbook → silver_orderbook."""
        conn = self.db.conn
        bronze = conn.execute("""
            SELECT b.* FROM bronze_orderbook b
            LEFT JOIN silver_orderbook s ON b.symbol = s.symbol AND b.timestamp = s.timestamp
            WHERE s.symbol IS NULL
        """).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM bronze_orderbook LIMIT 0").description]
        if not bronze:
            return 0

        now = datetime.now(timezone.utc)
        written = 0
        for row in bronze:
            rd = dict(zip(cols, row))
            bids = json.loads(rd["bids_json"]) if rd["bids_json"] else []
            asks = json.loads(rd["asks_json"]) if rd["asks_json"] else []
            best_bid = bids[0] if bids else None
            best_ask = asks[0] if asks else None
            bbp = best_bid["price"] if best_bid else None
            bap = best_ask["price"] if best_ask else None
            spread   = (bap - bbp) if bbp and bap else None
            mid      = ((bbp + bap) / 2.0) if bbp and bap else None
            dim = SYMBOL_DIM.get(rd["symbol"], {})
            conn.execute("""
                INSERT INTO silver_orderbook VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rd["symbol"], rd["timestamp"],
                bbp, best_bid["quantity"] if best_bid else None,
                bap, best_ask["quantity"] if best_ask else None,
                len(bids), len(asks),
                spread, mid,
                rd["sequence_number"],
                dim.get("company_name"), dim.get("sector"), now,
            ))
            written += 1
        return written

    # ── Stage 5: Gold Aggregation ─────────────────────────────────────────────

    def aggregate_gold(self) -> int:
        """Aggregate silver_trades → gold_daily_summary. Returns rows written."""
        conn = self.db.conn
        conn.execute("DELETE FROM gold_daily_summary")
        now = datetime.now(timezone.utc)
        conn.execute(f"""
            INSERT INTO gold_daily_summary
            SELECT
                symbol,
                CAST(timestamp AS DATE)                           AS trading_date,
                MIN(CASE WHEN rn_first = 1 THEN price END)        AS open_price,
                MIN(CASE WHEN rn_last  = 1 THEN price END)        AS close_price,
                MAX(price)                                         AS high_price,
                MIN(price)                                         AS low_price,
                SUM(price * quantity) / SUM(quantity)              AS vwap,
                SUM(quantity)                                      AS total_volume,
                COUNT(*)                                           AS trade_count,
                SUM(price * quantity)                              AS total_value,
                MAX(company_name)                                  AS company_name,
                MAX(sector)                                        AS sector,
                '{now.isoformat()}'::TIMESTAMPTZ                   AS _aggregated_at
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY symbol, CAST(timestamp AS DATE)
                                       ORDER BY timestamp ASC)  AS rn_first,
                    ROW_NUMBER() OVER (PARTITION BY symbol, CAST(timestamp AS DATE)
                                       ORDER BY timestamp DESC) AS rn_last
                FROM silver_trades
            ) t
            GROUP BY symbol, CAST(timestamp AS DATE)
        """)
        return self.db.count("gold_daily_summary")

    # ── Convenience: run all stages ───────────────────────────────────────────

    def run_full(self, raw_events: list[dict]) -> dict[str, int]:
        """Process a list of raw WebSocket events through all stages."""
        routed: list[tuple[str, dict]] = []
        rejected = 0
        for ev in raw_events:
            result = self.bridge_process(ev)
            if result is None:
                rejected += 1
            else:
                routed.append((result["topic"], result["flat"]))

        for topic, flat in routed:
            self.ingest_to_bronze(flat, topic)

        silver_trades   = self.process_silver_trades()
        silver_orderbook = self.process_silver_orderbook()
        gold_rows       = self.aggregate_gold()

        return {
            "events_in":       len(raw_events),
            "rejected":        rejected,
            "routed":          len(routed),
            "bronze_trades":   self.db.count("bronze_trades"),
            "bronze_orderbook":self.db.count("bronze_orderbook"),
            "silver_trades":   silver_trades,
            "silver_orderbook": silver_orderbook,
            "gold_rows":       gold_rows,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Session-scoped fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pipeline_db() -> PipelineDB:
    db = PipelineDB()
    yield db
    db.close()


@pytest.fixture(scope="session")
def simulator(pipeline_db: PipelineDB) -> PipelineSimulator:
    return PipelineSimulator(pipeline_db)


@pytest.fixture(scope="session")
def populated_pipeline(simulator: PipelineSimulator):
    """Run the full pipeline with a standard batch of events. Yields stats."""
    from tests.e2e.fixtures.events import make_trade_batch, make_orderbook_event

    events: list[dict] = []
    events += make_trade_batch(SYMBOLS, n_per_symbol=10)
    for sym in SYMBOLS:
        events.append(make_orderbook_event(sym))

    stats = simulator.run_full(events)
    return stats


@pytest.fixture(scope="session")
def symbols() -> list[str]:
    return SYMBOLS


@pytest.fixture(scope="session")
def symbol_dim() -> dict[str, dict]:
    return SYMBOL_DIM
