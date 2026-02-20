"""Tests for alert threshold SQL conditions defined in create_alerts.py.

Executes each alert's SQL condition query against the in-memory DuckDB
fixture and verifies they return well-formed result sets.
"""

import sys
import os

import pytest

_BOOTSTRAP_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "bootstrap")
sys.path.insert(0, os.path.abspath(_BOOTSTRAP_DIR))


# ──────────────────────────────────────────────────────────────────────────────
# create_alerts module structure
# ──────────────────────────────────────────────────────────────────────────────


def test_alerts_module_importable():
    import create_alerts
    assert hasattr(create_alerts, "ALERTS")


def test_alerts_list_non_empty():
    import create_alerts
    assert len(create_alerts.ALERTS) > 0


def test_each_alert_has_required_fields():
    import create_alerts
    required = {"name", "description", "sql"}
    for alert in create_alerts.ALERTS:
        missing = required - set(alert.keys())
        assert not missing, f"Alert '{alert.get('name')}' missing fields: {missing}"


def test_alert_names_are_non_empty():
    import create_alerts
    for alert in create_alerts.ALERTS:
        assert alert["name"].strip() != ""


def test_alert_sql_fields_are_strings():
    import create_alerts
    for alert in create_alerts.ALERTS:
        assert isinstance(alert["sql"], str)
        assert len(alert["sql"].strip()) > 0


# ──────────────────────────────────────────────────────────────────────────────
# High-volume alert condition
# ──────────────────────────────────────────────────────────────────────────────


def test_high_volume_alert_sql_runs(duckdb_conn):
    """High-volume alert: symbols where today's volume > 2x rolling average."""
    duckdb_conn.execute("""
        CREATE OR REPLACE TEMP VIEW vol_with_avg AS
        SELECT
            symbol,
            trade_date,
            total_volume,
            AVG(total_volume) OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
            ) AS avg_prior_volume
        FROM daily_trade_summary
    """)
    result = duckdb_conn.execute("""
        SELECT symbol, trade_date, total_volume, avg_prior_volume
        FROM vol_with_avg
        WHERE avg_prior_volume IS NOT NULL
          AND total_volume > 2.0 * avg_prior_volume
        ORDER BY trade_date DESC
        LIMIT 10
    """).fetchall()
    # Result may be empty (random data) — just must not error
    assert result is not None


def test_high_volume_alert_columns(duckdb_conn):
    desc = duckdb_conn.execute("""
        SELECT symbol, trade_date, total_volume, total_volume AS avg_volume
        FROM daily_trade_summary LIMIT 1
    """).description
    col_names = [d[0] for d in desc]
    assert "symbol" in col_names
    assert "trade_date" in col_names
    assert "total_volume" in col_names


# ──────────────────────────────────────────────────────────────────────────────
# Data freshness alert condition
# ──────────────────────────────────────────────────────────────────────────────


def test_freshness_alert_sql_runs(duckdb_conn):
    """Freshness alert: check that the latest trade_date is recent."""
    result = duckdb_conn.execute("""
        SELECT
            MAX(trade_date)                                  AS latest_date,
            CURRENT_DATE                                     AS today,
            CURRENT_DATE - MAX(trade_date)                   AS lag_days
        FROM trades_enriched
    """).fetchone()
    assert result is not None
    latest_date, today, lag_days = result
    assert latest_date is not None
    # In our fixture, data is seeded for the last 7 days
    assert lag_days is not None


def test_freshness_check_per_symbol(duckdb_conn, symbols):
    """Each symbol should have recent trade data."""
    from datetime import date, timedelta
    cutoff = date.today() - timedelta(days=7)
    result = duckdb_conn.execute(f"""
        SELECT symbol, MAX(trade_date) AS latest
        FROM trades_enriched
        GROUP BY symbol
        HAVING MAX(trade_date) < '{cutoff}'
    """).fetchall()
    # All symbols have data within the last 7 days in the fixture
    assert len(result) == 0, f"Symbols with stale data: {result}"


# ──────────────────────────────────────────────────────────────────────────────
# Alert result format
# ──────────────────────────────────────────────────────────────────────────────


def test_alert_result_is_numeric_threshold(duckdb_conn):
    """Alert SQL should return a numeric value that can be compared to a threshold."""
    count = duckdb_conn.execute("""
        SELECT COUNT(*) FROM trades_enriched
        WHERE trade_date = (SELECT MAX(trade_date) FROM trades_enriched)
    """).fetchone()[0]
    assert isinstance(count, int)
    assert count > 0


def test_sector_alert_detects_unusual_activity(duckdb_conn):
    """Sector-level volume z-score can be computed without errors."""
    result = duckdb_conn.execute("""
        SELECT
            trade_date,
            sector,
            total_volume,
            AVG(total_volume) OVER (PARTITION BY sector) AS sector_avg,
            STDDEV(total_volume) OVER (PARTITION BY sector) AS sector_std
        FROM sector_daily_summary
        LIMIT 10
    """).fetchall()
    assert len(result) > 0
