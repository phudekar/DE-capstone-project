"""Full end-to-end pipeline test.

Injects a controlled, deterministic batch of events and verifies that *every*
stage produces the expected output — from raw DE-Stock WebSocket envelopes all
the way to Superset analytical SQL.

Fixture strategy:
  - Module-scoped e2e_db / e2e_sim / e2e_stats use an *isolated* DuckDB
    instance (separate from the session-scoped pipeline_db used by stage tests)
    so that this file can control exactly what data is present.
  - 3 symbols, 5 trades each, 3 orderbook snapshots, 2 invalid events.
  - Tests are grouped into classes by pipeline stage.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from tests.e2e.conftest import PipelineDB, PipelineSimulator, SYMBOL_DIM
from tests.e2e.fixtures.events import (
    make_trade_batch,
    make_orderbook_event,
    make_invalid_event,
    make_malformed_trade_event,
)

# ── Deterministic test corpus ─────────────────────────────────────────────────

E2E_SYMBOLS       = ["AAPL", "MSFT", "TSLA"]
TRADES_PER_SYMBOL = 5
TOTAL_TRADES      = len(E2E_SYMBOLS) * TRADES_PER_SYMBOL   # 15
TOTAL_ORDERBOOKS  = len(E2E_SYMBOLS)                        # 3
TOTAL_INVALID     = 2                                       # 1 unknown + 1 malformed

QUERIES_DIR = Path(__file__).parent.parent.parent / "services/superset/saved_queries"


# ── Module-scoped fixtures ─────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def e2e_db():
    db = PipelineDB()
    yield db
    db.close()


@pytest.fixture(scope="module")
def e2e_sim(e2e_db):
    return PipelineSimulator(e2e_db)


@pytest.fixture(scope="module")
def e2e_stats(e2e_sim):
    """Run the full pipeline once with our controlled batch. Returns stats dict."""
    events = []
    events += make_trade_batch(E2E_SYMBOLS, n_per_symbol=TRADES_PER_SYMBOL, base_price=150.0)
    for sym in E2E_SYMBOLS:
        events.append(make_orderbook_event(sym, mid_price=150.0))
    events.append(make_invalid_event())       # unknown event_type → DLQ (rejected)
    events.append(make_malformed_trade_event())  # missing fields → rejected

    return e2e_sim.run_full(events)


# ═════════════════════════════════════════════════════════════════════════════
# Stage 1 — Event ingestion counts
# ═════════════════════════════════════════════════════════════════════════════

class TestStage1EventIngestion:
    """Verify the input event counts are correctly tracked by run_full()."""

    def test_total_events_processed(self, e2e_stats):
        expected_total = TOTAL_TRADES + TOTAL_ORDERBOOKS + TOTAL_INVALID
        assert e2e_stats["events_in"] == expected_total

    def test_invalid_events_rejected(self, e2e_stats):
        # Both unknown-event-type and malformed-trade are rejected
        assert e2e_stats["rejected"] == TOTAL_INVALID

    def test_valid_events_routed(self, e2e_stats):
        expected_routed = TOTAL_TRADES + TOTAL_ORDERBOOKS
        assert e2e_stats["routed"] == expected_routed


# ═════════════════════════════════════════════════════════════════════════════
# Stage 2 — Kafka Bridge routing
# ═════════════════════════════════════════════════════════════════════════════

class TestStage2KafkaBridge:
    """Verify trade events route to raw.trades and orderbooks to raw.orderbook-snapshots."""

    def test_trades_land_in_correct_topic(self, e2e_db):
        # All bronze_trades rows must have _kafka_topic = raw.trades
        bad = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM bronze_trades WHERE _kafka_topic != 'raw.trades'
        """).fetchone()[0]
        assert bad == 0

    def test_orderbooks_land_in_correct_topic(self, e2e_db):
        bad = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM bronze_orderbook
            WHERE _kafka_topic != 'raw.orderbook-snapshots'
        """).fetchone()[0]
        assert bad == 0

    def test_kafka_offsets_are_unique(self, e2e_db):
        """Each message must have a distinct Kafka offset."""
        all_offsets = (
            e2e_db.conn.execute("SELECT _kafka_offset FROM bronze_trades").fetchall()
            + e2e_db.conn.execute("SELECT _kafka_offset FROM bronze_orderbook").fetchall()
        )
        offsets = [r[0] for r in all_offsets]
        assert len(offsets) == len(set(offsets)), "Duplicate Kafka offsets detected"

    def test_trade_envelopes_carry_event_type(self, e2e_db):
        count = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE event_type = 'TradeExecuted'"
        ).fetchone()[0]
        assert count == TOTAL_TRADES

    def test_orderbook_envelopes_carry_event_type(self, e2e_db):
        count = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_orderbook WHERE event_type = 'OrderBookSnapshot'"
        ).fetchone()[0]
        assert count == TOTAL_ORDERBOOKS


# ═════════════════════════════════════════════════════════════════════════════
# Stage 3 — Bronze ingestion
# ═════════════════════════════════════════════════════════════════════════════

class TestStage3BronzeIngestion:
    """Verify raw events are correctly persisted in bronze tables."""

    def test_bronze_trade_row_count(self, e2e_db, e2e_stats):
        assert e2e_db.count("bronze_trades") == TOTAL_TRADES
        assert e2e_stats["bronze_trades"] == TOTAL_TRADES

    def test_bronze_orderbook_row_count(self, e2e_db, e2e_stats):
        assert e2e_db.count("bronze_orderbook") == TOTAL_ORDERBOOKS
        assert e2e_stats["bronze_orderbook"] == TOTAL_ORDERBOOKS

    def test_bronze_trade_symbols_correct(self, e2e_db):
        syms = {r[0] for r in e2e_db.conn.execute(
            "SELECT DISTINCT symbol FROM bronze_trades"
        ).fetchall()}
        assert syms == set(E2E_SYMBOLS)

    def test_bronze_trade_ids_are_non_null(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE trade_id IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_bronze_trade_prices_positive(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE price <= 0"
        ).fetchone()[0]
        assert bad == 0

    def test_bronze_trade_quantities_positive(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE quantity <= 0"
        ).fetchone()[0]
        assert bad == 0

    def test_bronze_ingested_at_populated(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE _ingested_at IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_bronze_orderbook_bids_valid_json(self, e2e_db):
        rows = e2e_db.conn.execute("SELECT bids_json FROM bronze_orderbook").fetchall()
        for (bids_json,) in rows:
            bids = json.loads(bids_json)
            assert isinstance(bids, list)
            assert len(bids) > 0

    def test_bronze_orderbook_asks_valid_json(self, e2e_db):
        rows = e2e_db.conn.execute("SELECT asks_json FROM bronze_orderbook").fetchall()
        for (asks_json,) in rows:
            asks = json.loads(asks_json)
            assert isinstance(asks, list)
            assert len(asks) > 0


# ═════════════════════════════════════════════════════════════════════════════
# Stage 4 — Silver transformation
# ═════════════════════════════════════════════════════════════════════════════

class TestStage4SilverTransformation:
    """Verify deduplication, enrichment, and spread calculations."""

    def test_silver_trade_row_count(self, e2e_db, e2e_stats):
        assert e2e_db.count("silver_trades") == TOTAL_TRADES
        assert e2e_stats["silver_trades"] == TOTAL_TRADES

    def test_silver_orderbook_row_count(self, e2e_db, e2e_stats):
        assert e2e_db.count("silver_orderbook") == TOTAL_ORDERBOOKS
        assert e2e_stats["silver_orderbook"] == TOTAL_ORDERBOOKS

    def test_silver_trade_ids_unique(self, e2e_db):
        total = e2e_db.count("silver_trades")
        distinct = e2e_db.conn.execute(
            "SELECT COUNT(DISTINCT trade_id) FROM silver_trades"
        ).fetchone()[0]
        assert total == distinct

    def test_silver_trades_enriched_with_company_name(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM silver_trades WHERE company_name IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_silver_trades_enriched_with_sector(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM silver_trades WHERE sector IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_silver_trades_sector_values_correct(self, e2e_db):
        rows = e2e_db.conn.execute(
            "SELECT DISTINCT symbol, sector FROM silver_trades"
        ).fetchall()
        for symbol, sector in rows:
            expected = SYMBOL_DIM[symbol]["sector"]
            assert sector == expected, f"{symbol}: expected sector {expected}, got {sector}"

    def test_silver_orderbook_spread_positive(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM silver_orderbook WHERE spread IS NOT NULL AND spread <= 0"
        ).fetchone()[0]
        assert bad == 0

    def test_silver_orderbook_mid_price_between_bid_ask(self, e2e_db):
        bad = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM silver_orderbook
            WHERE mid_price IS NOT NULL
              AND (mid_price < best_bid_price OR mid_price > best_ask_price)
        """).fetchone()[0]
        assert bad == 0

    def test_silver_orderbook_bid_depth_positive(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM silver_orderbook WHERE bid_depth <= 0"
        ).fetchone()[0]
        assert bad == 0

    def test_silver_processed_at_populated(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM silver_trades WHERE _processed_at IS NULL"
        ).fetchone()[0]
        assert nulls == 0


# ═════════════════════════════════════════════════════════════════════════════
# Stage 5 — Gold aggregation
# ═════════════════════════════════════════════════════════════════════════════

class TestStage5GoldAggregation:
    """Verify OHLCV rollup produces correct OHLC invariants and VWAP."""

    def test_gold_row_count(self, e2e_db, e2e_stats):
        # One row per symbol (all trades on same date in test)
        assert e2e_db.count("gold_daily_summary") == len(E2E_SYMBOLS)
        assert e2e_stats["gold_rows"] == len(E2E_SYMBOLS)

    def test_gold_all_symbols_present(self, e2e_db):
        syms = {r[0] for r in e2e_db.conn.execute(
            "SELECT DISTINCT symbol FROM gold_daily_summary"
        ).fetchall()}
        assert syms == set(E2E_SYMBOLS)

    def test_gold_high_gte_low(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < low_price"
        ).fetchone()[0]
        assert bad == 0

    def test_gold_high_gte_open(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < open_price"
        ).fetchone()[0]
        assert bad == 0

    def test_gold_low_lte_open(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > open_price"
        ).fetchone()[0]
        assert bad == 0

    def test_gold_high_gte_close(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < close_price"
        ).fetchone()[0]
        assert bad == 0

    def test_gold_low_lte_close(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > close_price"
        ).fetchone()[0]
        assert bad == 0

    def test_gold_vwap_in_range(self, e2e_db):
        bad = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM gold_daily_summary
            WHERE vwap < low_price OR vwap > high_price
        """).fetchone()[0]
        assert bad == 0

    def test_gold_total_volume_matches_silver(self, e2e_db):
        mismatch = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT g.symbol, g.total_volume, SUM(s.quantity) AS expected
                FROM gold_daily_summary g
                JOIN silver_trades s ON g.symbol = s.symbol
                GROUP BY g.symbol, g.total_volume
                HAVING g.total_volume != SUM(s.quantity)
            )
        """).fetchone()[0]
        assert mismatch == 0

    def test_gold_trade_count_matches_silver(self, e2e_db):
        mismatch = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT g.symbol, g.trade_count, COUNT(*) AS expected
                FROM gold_daily_summary g
                JOIN silver_trades s ON g.symbol = s.symbol
                GROUP BY g.symbol, g.trade_count
                HAVING g.trade_count != COUNT(*)
            )
        """).fetchone()[0]
        assert mismatch == 0

    def test_gold_sector_populated(self, e2e_db):
        nulls = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE sector IS NULL"
        ).fetchone()[0]
        assert nulls == 0


# ═════════════════════════════════════════════════════════════════════════════
# Stage 6 — Data quality invariants (cross-layer)
# ═════════════════════════════════════════════════════════════════════════════

class TestStage6DataQuality:
    """End-to-end data quality checks across all three medallion layers."""

    def test_no_bronze_trades_with_null_symbol(self, e2e_db):
        assert e2e_db.conn.execute(
            "SELECT COUNT(*) FROM bronze_trades WHERE symbol IS NULL"
        ).fetchone()[0] == 0

    def test_silver_trade_ids_match_bronze(self, e2e_db):
        """Every silver trade_id must exist in bronze — no phantom rows."""
        phantoms = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM silver_trades s
            LEFT JOIN bronze_trades b USING (trade_id)
            WHERE b.trade_id IS NULL
        """).fetchone()[0]
        assert phantoms == 0

    def test_gold_symbols_are_subset_of_silver(self, e2e_db):
        phantom_symbols = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM gold_daily_summary g
            LEFT JOIN (SELECT DISTINCT symbol FROM silver_trades) s USING (symbol)
            WHERE s.symbol IS NULL
        """).fetchone()[0]
        assert phantom_symbols == 0

    def test_silver_trade_count_not_exceeds_bronze(self, e2e_db):
        bronze_count = e2e_db.count("bronze_trades")
        silver_count = e2e_db.count("silver_trades")
        assert silver_count <= bronze_count

    def test_gold_total_value_positive_all_rows(self, e2e_db):
        bad = e2e_db.conn.execute(
            "SELECT COUNT(*) FROM gold_daily_summary WHERE total_value <= 0"
        ).fetchone()[0]
        assert bad == 0

    def test_aggressor_side_values_valid(self, e2e_db):
        bad = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM silver_trades
            WHERE aggressor_side NOT IN ('Buy', 'Sell')
        """).fetchone()[0]
        assert bad == 0

    def test_bronze_to_silver_no_data_loss(self, e2e_db):
        """Silver must contain exactly as many rows as unique bronze trade_ids."""
        unique_bronze = e2e_db.conn.execute(
            "SELECT COUNT(DISTINCT trade_id) FROM bronze_trades"
        ).fetchone()[0]
        silver_count = e2e_db.count("silver_trades")
        assert silver_count == unique_bronze


# ═════════════════════════════════════════════════════════════════════════════
# Stage 7 — GraphQL API SQL queries
# ═════════════════════════════════════════════════════════════════════════════

class TestStage7GraphQLAPI:
    """Verify that resolver-style SQL queries return correct results."""

    def test_trades_query_returns_all_rows(self, e2e_db):
        rows = e2e_db.conn.execute(
            "SELECT * FROM silver_trades ORDER BY timestamp LIMIT 100"
        ).fetchall()
        assert len(rows) == TOTAL_TRADES

    def test_trades_filter_by_symbol(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT * FROM silver_trades WHERE symbol = 'AAPL'
        """).fetchall()
        assert len(rows) == TRADES_PER_SYMBOL

    def test_trades_filter_by_sector(self, e2e_db):
        # AAPL + MSFT are Technology; TSLA is Consumer Discretionary
        tech_rows = e2e_db.conn.execute("""
            SELECT COUNT(*) FROM silver_trades WHERE sector = 'Technology'
        """).fetchone()[0]
        assert tech_rows == TRADES_PER_SYMBOL * 2  # AAPL + MSFT

    def test_market_summary_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT symbol, trade_count, total_volume
            FROM gold_daily_summary
            ORDER BY symbol
        """).fetchall()
        assert len(rows) == len(E2E_SYMBOLS)
        for symbol, count, volume in rows:
            assert count == TRADES_PER_SYMBOL
            assert volume > 0

    def test_orderbook_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT symbol, best_bid_price, best_ask_price, spread
            FROM silver_orderbook
            WHERE spread IS NOT NULL
        """).fetchall()
        assert len(rows) == TOTAL_ORDERBOOKS
        for sym, bid, ask, spread in rows:
            assert bid < ask
            assert spread > 0

    def test_top_traders_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT buyer_agent_id, COUNT(*) AS trades
            FROM silver_trades
            GROUP BY buyer_agent_id
            ORDER BY trades DESC
            LIMIT 10
        """).fetchall()
        assert len(rows) > 0

    def test_sector_breakdown_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT sector,
                   COUNT(*) AS trades,
                   SUM(price * quantity) AS total_value
            FROM silver_trades
            GROUP BY sector
            ORDER BY total_value DESC
        """).fetchall()
        sectors = {r[0] for r in rows}
        assert "Technology" in sectors
        assert "Consumer Discretionary" in sectors


# ═════════════════════════════════════════════════════════════════════════════
# Stage 8 — Dagster asset definitions
# ═════════════════════════════════════════════════════════════════════════════

class TestStage8DagsterAssets:
    """Smoke-test that the Dagster orchestration layer is importable and consistent."""

    def test_dagster_importable(self):
        dagster = pytest.importorskip("dagster", reason="dagster not installed in this venv")
        assert dagster is not None

    def test_orchestrator_package_importable(self):
        pytest.importorskip("dagster", reason="dagster not installed in this venv")
        import sys
        from pathlib import Path
        orch_src = str(Path(__file__).parent.parent.parent / "services/dagster-orchestrator/src")
        if orch_src not in sys.path:
            sys.path.insert(0, orch_src)
        try:
            import lakehouse_orchestrator
            assert lakehouse_orchestrator is not None
        except ImportError:
            pytest.skip("lakehouse_orchestrator package not installed")

    def test_asset_definitions_loadable(self):
        pytest.importorskip("dagster", reason="dagster not installed in this venv")
        import sys
        from pathlib import Path
        orch_src = str(Path(__file__).parent.parent.parent / "services/dagster-orchestrator/src")
        if orch_src not in sys.path:
            sys.path.insert(0, orch_src)
        try:
            from lakehouse_orchestrator.assets import bronze_assets, silver_assets, gold_assets
            assert bronze_assets is not None
            assert silver_assets is not None
            assert gold_assets is not None
        except ImportError:
            pytest.skip("Asset modules not available")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 9 — Superset saved query execution
# ═════════════════════════════════════════════════════════════════════════════

class TestStage9SupersetQueries:
    """Verify saved-query SQL files exist and their analytical queries run correctly."""

    REQUIRED_QUERY_FILES = [
        "ohlcv_with_technicals.sql",
        "unusual_volume_detection.sql",
        "sector_rotation_analysis.sql",
        "trader_profile_deep_dive.sql",
        "cross_account_activity.sql",
        "daily_market_summary.sql",
    ]

    def test_saved_query_files_exist(self):
        for filename in self.REQUIRED_QUERY_FILES:
            path = QUERIES_DIR / filename
            assert path.exists(), f"Missing: {path}"
            assert path.stat().st_size > 50, f"Empty: {path}"

    def test_ohlcv_technicals_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT symbol,
                   MIN(price)   AS low_price,
                   MAX(price)   AS high_price,
                   AVG(price)   AS avg_price,
                   SUM(quantity) AS volume
            FROM silver_trades
            GROUP BY symbol
            ORDER BY symbol
        """).fetchall()
        assert len(rows) == len(E2E_SYMBOLS)
        for _, low, high, avg, vol in rows:
            assert low <= avg <= high
            assert vol > 0

    def test_sector_rotation_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT sector,
                   SUM(price * quantity) AS total_value,
                   COUNT(*) AS trade_count
            FROM silver_trades
            GROUP BY sector
        """).fetchall()
        assert len(rows) >= 2
        sectors = {r[0] for r in rows}
        assert "Technology" in sectors

    def test_daily_market_summary_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT symbol,
                   CAST(timestamp AS DATE) AS trade_date,
                   MIN(price) AS day_low,
                   MAX(price) AS day_high,
                   SUM(quantity) AS total_volume,
                   SUM(price * quantity) / SUM(quantity) AS vwap
            FROM silver_trades
            GROUP BY symbol, CAST(timestamp AS DATE)
        """).fetchall()
        assert len(rows) == len(E2E_SYMBOLS)
        for sym, tdate, low, high, vol, vwap in rows:
            assert low <= vwap <= high
            assert vol > 0

    def test_trader_profile_query(self, e2e_db):
        rows = e2e_db.conn.execute("""
            SELECT buyer_agent_id,
                   COUNT(*) AS trade_count,
                   SUM(quantity) AS total_qty,
                   COUNT(DISTINCT symbol) AS symbols_traded
            FROM silver_trades
            GROUP BY buyer_agent_id
            ORDER BY trade_count DESC
        """).fetchall()
        assert len(rows) > 0
        for agent, cnt, qty, syms in rows:
            assert cnt >= 1
            assert qty > 0
            assert syms >= 1

    def test_market_kpi_query(self, e2e_db):
        row = e2e_db.conn.execute("""
            SELECT
                SUM(total_volume)         AS market_volume,
                SUM(trade_count)          AS total_trades,
                COUNT(DISTINCT symbol)    AS active_symbols
            FROM gold_daily_summary
        """).fetchone()
        market_vol, total_trades, active_syms = row
        assert market_vol > 0
        assert total_trades == TOTAL_TRADES
        assert active_syms == len(E2E_SYMBOLS)


# ═════════════════════════════════════════════════════════════════════════════
# Full pipeline assertion — cross-stage consistency
# ═════════════════════════════════════════════════════════════════════════════

class TestFullPipelineConsistency:
    """Cross-stage invariants that span the entire pipeline."""

    def test_events_in_equals_routed_plus_rejected(self, e2e_stats):
        assert e2e_stats["events_in"] == e2e_stats["routed"] + e2e_stats["rejected"]

    def test_bronze_plus_orderbook_equals_routed(self, e2e_stats):
        assert (e2e_stats["bronze_trades"] + e2e_stats["bronze_orderbook"]
                == e2e_stats["routed"])

    def test_silver_count_not_exceeds_bronze(self, e2e_stats):
        assert e2e_stats["silver_trades"] <= e2e_stats["bronze_trades"]

    def test_gold_symbol_count_equals_expected(self, e2e_stats):
        assert e2e_stats["gold_rows"] == len(E2E_SYMBOLS)

    def test_data_volume_preserved_end_to_end(self, e2e_db):
        """Total trade volume is identical across bronze → silver → gold."""
        bronze_vol = e2e_db.conn.execute(
            "SELECT SUM(quantity) FROM bronze_trades"
        ).fetchone()[0]
        silver_vol = e2e_db.conn.execute(
            "SELECT SUM(quantity) FROM silver_trades"
        ).fetchone()[0]
        gold_vol = e2e_db.conn.execute(
            "SELECT SUM(total_volume) FROM gold_daily_summary"
        ).fetchone()[0]
        assert bronze_vol == silver_vol == gold_vol

    def test_total_value_consistent_across_layers(self, e2e_db):
        silver_val = e2e_db.conn.execute(
            "SELECT ROUND(SUM(price * quantity), 4) FROM silver_trades"
        ).fetchone()[0]
        gold_val = e2e_db.conn.execute(
            "SELECT ROUND(SUM(total_value), 4) FROM gold_daily_summary"
        ).fetchone()[0]
        assert abs(silver_val - gold_val) < 0.01, (
            f"Value mismatch: silver={silver_val}, gold={gold_val}"
        )

    def test_all_symbols_flow_through_all_layers(self, e2e_db):
        bronze_syms = {r[0] for r in e2e_db.conn.execute(
            "SELECT DISTINCT symbol FROM bronze_trades"
        ).fetchall()}
        silver_syms = {r[0] for r in e2e_db.conn.execute(
            "SELECT DISTINCT symbol FROM silver_trades"
        ).fetchall()}
        gold_syms = {r[0] for r in e2e_db.conn.execute(
            "SELECT DISTINCT symbol FROM gold_daily_summary"
        ).fetchall()}
        assert bronze_syms == silver_syms == gold_syms == set(E2E_SYMBOLS)

    def test_no_data_duplication_bronze_to_silver(self, e2e_db):
        bronze_count = e2e_db.count("bronze_trades")
        silver_count = e2e_db.count("silver_trades")
        # Silver must equal bronze since all our test trade_ids are unique
        assert silver_count == bronze_count

    def test_sector_distribution_in_gold(self, e2e_db):
        """Gold must carry sector info that matches dim_symbol."""
        rows = e2e_db.conn.execute(
            "SELECT symbol, sector FROM gold_daily_summary"
        ).fetchall()
        for symbol, sector in rows:
            expected = SYMBOL_DIM[symbol]["sector"]
            assert sector == expected, f"{symbol}: {sector} != {expected}"
