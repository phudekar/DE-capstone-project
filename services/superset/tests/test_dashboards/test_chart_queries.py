"""Tests that the SQL queries backing each Superset chart return valid results.

Each test executes the underlying SQL against the in-memory DuckDB fixture and
verifies schema, row counts, and data types — without requiring a live Superset
or Iceberg cluster.
"""

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# daily_trade_summary view
# ──────────────────────────────────────────────────────────────────────────────


def test_daily_trade_summary_returns_rows(duckdb_conn):
    rows = duckdb_conn.execute("SELECT * FROM daily_trade_summary").fetchall()
    assert len(rows) > 0


def test_daily_trade_summary_columns(duckdb_conn):
    cols = {d[0] for d in duckdb_conn.execute("SELECT * FROM daily_trade_summary LIMIT 1").description}
    required = {"trade_date", "symbol", "trade_count", "total_volume", "total_value",
                "low_price", "high_price", "avg_price", "sector"}
    assert required.issubset(cols)


def test_daily_trade_summary_no_nulls_in_key_cols(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM daily_trade_summary
        WHERE trade_date IS NULL OR symbol IS NULL OR trade_count IS NULL
    """).fetchone()[0]
    assert result == 0


def test_daily_trade_summary_positive_volume(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM daily_trade_summary WHERE total_volume <= 0
    """).fetchone()[0]
    assert result == 0


def test_daily_trade_summary_low_le_high(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM daily_trade_summary WHERE low_price > high_price
    """).fetchone()[0]
    assert result == 0


# ──────────────────────────────────────────────────────────────────────────────
# market_daily_overview view
# ──────────────────────────────────────────────────────────────────────────────


def test_market_overview_one_row_per_day(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT trade_date, COUNT(*) AS cnt
        FROM market_daily_overview
        GROUP BY trade_date
        HAVING cnt > 1
    """).fetchall()
    assert len(result) == 0, "Duplicate dates in market_daily_overview"


def test_market_overview_positive_trades(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM market_daily_overview WHERE total_trades <= 0
    """).fetchone()[0]
    assert result == 0


def test_market_overview_active_symbols_correct(duckdb_conn, symbols):
    result = duckdb_conn.execute("""
        SELECT MAX(active_symbols) FROM market_daily_overview
    """).fetchone()[0]
    assert result == len(symbols)


# ──────────────────────────────────────────────────────────────────────────────
# sector_daily_summary view
# ──────────────────────────────────────────────────────────────────────────────


def test_sector_summary_returns_rows(duckdb_conn):
    rows = duckdb_conn.execute("SELECT * FROM sector_daily_summary").fetchall()
    assert len(rows) > 0


def test_sector_summary_known_sectors(duckdb_conn):
    sectors = {r[0] for r in duckdb_conn.execute(
        "SELECT DISTINCT sector FROM sector_daily_summary"
    ).fetchall()}
    assert "Technology" in sectors
    assert "Consumer Discretionary" in sectors


def test_sector_summary_no_negative_volume(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM sector_daily_summary WHERE total_volume < 0
    """).fetchone()[0]
    assert result == 0


# ──────────────────────────────────────────────────────────────────────────────
# trades_enriched table
# ──────────────────────────────────────────────────────────────────────────────


def test_trades_enriched_row_count(duckdb_conn, symbols):
    count = duckdb_conn.execute("SELECT COUNT(*) FROM trades_enriched").fetchone()[0]
    # 7 days × 5 symbols × 30 trades
    assert count == 7 * len(symbols) * 30


def test_trades_enriched_valid_trade_types(duckdb_conn):
    invalid = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trades_enriched
        WHERE trade_type NOT IN ('BUY', 'SELL')
    """).fetchone()[0]
    assert invalid == 0


def test_trades_enriched_positive_price(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trades_enriched WHERE price <= 0
    """).fetchone()[0]
    assert result == 0


def test_trades_enriched_value_matches_price_qty(duckdb_conn):
    """value should equal price * quantity (within floating-point tolerance)."""
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trades_enriched
        WHERE ABS(value - price * quantity) > 0.02
    """).fetchone()[0]
    assert result == 0


def test_trades_enriched_all_symbols_present(duckdb_conn, symbols):
    found = {r[0] for r in duckdb_conn.execute(
        "SELECT DISTINCT symbol FROM trades_enriched"
    ).fetchall()}
    assert set(symbols) == found


# ──────────────────────────────────────────────────────────────────────────────
# symbol_reference table
# ──────────────────────────────────────────────────────────────────────────────


def test_symbol_reference_has_all_symbols(duckdb_conn, symbols):
    found = {r[0] for r in duckdb_conn.execute(
        "SELECT symbol FROM symbol_reference"
    ).fetchall()}
    assert set(symbols) == found


def test_symbol_reference_positive_market_cap(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM symbol_reference WHERE market_cap <= 0
    """).fetchone()[0]
    assert result == 0


# ──────────────────────────────────────────────────────────────────────────────
# trader_performance_metrics view
# ──────────────────────────────────────────────────────────────────────────────


def test_trader_metrics_returns_rows(duckdb_conn):
    rows = duckdb_conn.execute("SELECT * FROM trader_performance_metrics").fetchall()
    assert len(rows) > 0


def test_trader_metrics_non_negative_quantities(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trader_performance_metrics
        WHERE bought_qty < 0 OR sold_qty < 0
    """).fetchone()[0]
    assert result == 0


def test_trader_metrics_dates_ordered(duckdb_conn):
    result = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trader_performance_metrics
        WHERE first_trade_date > last_trade_date
    """).fetchone()[0]
    assert result == 0
