"""Stage 4 — Silver transformation: Bronze → Silver (dedup + enrich).

Tests:
  - Deduplication: same trade_id appears only once in Silver
  - Enrichment: company_name and sector from dim_symbol are applied
  - Orderbook: spread and mid_price computed correctly
  - No nulls on required Silver fields
  - Running silver twice produces no additional rows (idempotent)
"""

import pytest


# ── Trade deduplication ────────────────────────────────────────────────────────

def test_silver_trades_no_duplicate_trade_ids(pipeline_db, populated_pipeline):
    dups = pipeline_db.conn.execute("""
        SELECT trade_id, COUNT(*) AS cnt
        FROM silver_trades
        GROUP BY trade_id HAVING cnt > 1
    """).fetchall()
    assert len(dups) == 0, f"Duplicate trade_ids in Silver: {dups}"


def test_silver_trades_count_lte_bronze(pipeline_db, populated_pipeline):
    bronze = pipeline_db.count("bronze_trades")
    silver = pipeline_db.count("silver_trades")
    assert silver <= bronze


def test_silver_trades_all_bronze_ids_present(pipeline_db, populated_pipeline):
    missing = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT trade_id FROM bronze_trades
            EXCEPT
            SELECT trade_id FROM silver_trades
        )
    """).fetchone()[0]
    assert missing == 0


# ── Symbol enrichment ─────────────────────────────────────────────────────────

def test_silver_trades_company_name_populated(pipeline_db, populated_pipeline):
    nulls = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_trades WHERE company_name IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_silver_trades_sector_populated(pipeline_db, populated_pipeline):
    nulls = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_trades WHERE sector IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_silver_aapl_enriched_correctly(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute("""
        SELECT company_name, sector FROM silver_trades
        WHERE symbol = 'AAPL' LIMIT 1
    """).fetchone()
    assert row is not None
    assert row[0] == "Apple Inc."
    assert row[1] == "Technology"


def test_silver_amzn_sector_consumer(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute(
        "SELECT sector FROM silver_trades WHERE symbol = 'AMZN' LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "Consumer Discretionary"


def test_silver_jpm_sector_financials(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute(
        "SELECT sector FROM silver_trades WHERE symbol = 'JPM' LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] == "Financials"


# ── Required field integrity ───────────────────────────────────────────────────

def test_silver_no_null_required_fields(pipeline_db, populated_pipeline):
    for col in ("trade_id", "symbol", "price", "quantity",
                "timestamp", "_processed_at"):
        bad = pipeline_db.conn.execute(
            f"SELECT COUNT(*) FROM silver_trades WHERE {col} IS NULL"
        ).fetchone()[0]
        assert bad == 0, f"NULL found in required column: silver_trades.{col}"


def test_silver_positive_prices(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_trades WHERE price <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_silver_valid_aggressor_sides(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades
        WHERE aggressor_side NOT IN ('Buy', 'Sell')
    """).fetchone()[0]
    assert bad == 0


def test_silver_processed_at_is_set(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_trades WHERE _processed_at IS NULL"
    ).fetchone()[0]
    assert bad == 0


# ── Price / quantity unchanged from Bronze ────────────────────────────────────

def test_silver_price_unchanged_from_bronze(pipeline_db, populated_pipeline):
    mismatches = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE ABS(s.price - b.price) > 0.001
    """).fetchone()[0]
    assert mismatches == 0


def test_silver_quantity_unchanged_from_bronze(pipeline_db, populated_pipeline):
    mismatches = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE s.quantity != b.quantity
    """).fetchone()[0]
    assert mismatches == 0


# ── Orderbook silver ──────────────────────────────────────────────────────────

def test_silver_orderbook_row_count(pipeline_db, populated_pipeline):
    assert pipeline_db.count("silver_orderbook") == 6


def test_silver_orderbook_spread_positive(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_orderbook WHERE spread <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_silver_orderbook_mid_price_between_bid_ask(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_orderbook
        WHERE mid_price IS NOT NULL
          AND (mid_price < best_bid_price OR mid_price > best_ask_price)
    """).fetchone()[0]
    assert bad == 0


def test_silver_orderbook_bid_lt_ask(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_orderbook WHERE best_bid_price >= best_ask_price"
    ).fetchone()[0]
    assert bad == 0


def test_silver_orderbook_enriched(pipeline_db, populated_pipeline):
    nulls = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM silver_orderbook WHERE sector IS NULL"
    ).fetchone()[0]
    assert nulls == 0


# ── Idempotency ────────────────────────────────────────────────────────────────

def test_silver_processing_is_idempotent(simulator, pipeline_db):
    count_before = pipeline_db.count("silver_trades")
    written = simulator.process_silver_trades()
    count_after = pipeline_db.count("silver_trades")
    assert written == 0, "Re-running silver should write 0 new rows (idempotent)"
    assert count_after == count_before
