"""Stage 5 — Gold aggregation: Silver → Gold (daily OHLCV).

Tests:
  - One gold row per symbol per trading date
  - OHLC ordering invariants: low ≤ open/close ≤ high
  - VWAP = Σ(price × qty) / Σ(qty)
  - total_value = Σ(price × qty)
  - trade_count matches silver row count per symbol-date group
  - All symbols have gold rows
"""

import pytest


# ── Row count / coverage ───────────────────────────────────────────────────────

def test_gold_has_rows(pipeline_db, populated_pipeline):
    assert pipeline_db.count("gold_daily_summary") > 0


def test_gold_one_row_per_symbol_per_date(pipeline_db, populated_pipeline):
    dups = pipeline_db.conn.execute("""
        SELECT symbol, trading_date, COUNT(*) AS cnt
        FROM gold_daily_summary
        GROUP BY symbol, trading_date HAVING cnt > 1
    """).fetchall()
    assert len(dups) == 0, f"Duplicate symbol-date in gold: {dups}"


def test_gold_all_symbols_present(pipeline_db, populated_pipeline, symbols):
    found = {r[0] for r in pipeline_db.conn.execute(
        "SELECT DISTINCT symbol FROM gold_daily_summary"
    ).fetchall()}
    assert set(symbols) == found


# ── OHLC validity ─────────────────────────────────────────────────────────────

def test_gold_high_gte_open(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < open_price"
    ).fetchone()[0]
    assert bad == 0


def test_gold_high_gte_close(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < close_price"
    ).fetchone()[0]
    assert bad == 0


def test_gold_low_lte_open(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > open_price"
    ).fetchone()[0]
    assert bad == 0


def test_gold_low_lte_close(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > close_price"
    ).fetchone()[0]
    assert bad == 0


def test_gold_low_lte_high(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > high_price"
    ).fetchone()[0]
    assert bad == 0


# ── Volume / value / count ────────────────────────────────────────────────────

def test_gold_positive_volume(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE total_volume <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_gold_positive_trade_count(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE trade_count <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_gold_total_volume_matches_silver(pipeline_db, populated_pipeline):
    mismatch = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT g.symbol, g.trading_date, g.total_volume,
                   SUM(s.quantity) AS expected
            FROM gold_daily_summary g
            JOIN silver_trades s
              ON g.symbol = s.symbol
             AND g.trading_date = CAST(s.timestamp AS DATE)
            GROUP BY g.symbol, g.trading_date, g.total_volume
            HAVING ABS(g.total_volume - SUM(s.quantity)) > 0
        )
    """).fetchone()[0]
    assert mismatch == 0


def test_gold_trade_count_matches_silver(pipeline_db, populated_pipeline):
    mismatch = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT g.symbol, g.trading_date, g.trade_count, COUNT(*) AS expected
            FROM gold_daily_summary g
            JOIN silver_trades s
              ON g.symbol = s.symbol
             AND g.trading_date = CAST(s.timestamp AS DATE)
            GROUP BY g.symbol, g.trading_date, g.trade_count
            HAVING g.trade_count != COUNT(*)
        )
    """).fetchone()[0]
    assert mismatch == 0


def test_gold_total_value_correct(pipeline_db, populated_pipeline):
    mismatch = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT g.symbol, g.trading_date, g.total_value,
                   SUM(s.price * s.quantity) AS expected
            FROM gold_daily_summary g
            JOIN silver_trades s
              ON g.symbol = s.symbol
             AND g.trading_date = CAST(s.timestamp AS DATE)
            GROUP BY g.symbol, g.trading_date, g.total_value
            HAVING ABS(g.total_value - SUM(s.price * s.quantity)) > 0.01
        )
    """).fetchone()[0]
    assert mismatch == 0


# ── VWAP correctness ──────────────────────────────────────────────────────────

def test_gold_vwap_in_price_range(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM gold_daily_summary
        WHERE vwap < low_price OR vwap > high_price
    """).fetchone()[0]
    assert bad == 0


def test_gold_vwap_equals_weighted_average(pipeline_db, populated_pipeline):
    mismatch = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT g.symbol, g.vwap,
                   SUM(s.price * s.quantity) / SUM(s.quantity) AS expected_vwap
            FROM gold_daily_summary g
            JOIN silver_trades s
              ON g.symbol = s.symbol
             AND g.trading_date = CAST(s.timestamp AS DATE)
            GROUP BY g.symbol, g.vwap
            HAVING ABS(g.vwap - SUM(s.price * s.quantity) / SUM(s.quantity)) > 0.001
        )
    """).fetchone()[0]
    assert mismatch == 0


# ── Enrichment carried through ────────────────────────────────────────────────

def test_gold_sector_populated(pipeline_db, populated_pipeline):
    nulls = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE sector IS NULL"
    ).fetchone()[0]
    assert nulls == 0


def test_gold_company_name_populated(pipeline_db, populated_pipeline):
    nulls = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE company_name IS NULL"
    ).fetchone()[0]
    assert nulls == 0
