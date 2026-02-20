"""Stage 9 — Superset saved-query validation.

Runs the analytical SQL from each Superset saved-query file against the
DuckDB pipeline state (adapted for our table names) to verify correctness.
Also checks that every saved-query file exists and is non-empty.
"""

import re
from pathlib import Path

import pytest

QUERIES_DIR = Path(__file__).parent.parent.parent / "services/superset/saved_queries"


@pytest.mark.parametrize("filename", [
    "ohlcv_with_technicals.sql",
    "unusual_volume_detection.sql",
    "sector_rotation_analysis.sql",
    "trader_profile_deep_dive.sql",
    "cross_account_activity.sql",
    "daily_market_summary.sql",
])
def test_saved_query_file_exists_and_non_empty(filename):
    path = QUERIES_DIR / filename
    assert path.exists(), f"Missing saved query file: {filename}"
    assert path.stat().st_size > 100, f"Saved query file looks empty: {filename}"


# ── OHLCV with technicals ─────────────────────────────────────────────────────

def test_ohlcv_technicals_sql(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT
            CAST(timestamp AS DATE)  AS trade_date,
            MIN(price)               AS low_price,
            MAX(price)               AS high_price,
            AVG(price)               AS avg_price,
            SUM(quantity)            AS volume,
            COUNT(*)                 AS trade_count
        FROM silver_trades
        WHERE symbol = 'AAPL'
        GROUP BY CAST(timestamp AS DATE)
        ORDER BY trade_date
        LIMIT 30
    """).fetchall()
    assert len(rows) > 0
    for row in rows:
        trade_date, low, high, avg, vol, cnt = row
        assert low <= high
        assert low <= avg <= high
        assert vol > 0


def test_sma_window_query(pipeline_db, populated_pipeline):
    """SMA-20 window function must run without error."""
    rows = pipeline_db.conn.execute("""
        WITH daily AS (
            SELECT symbol,
                   CAST(timestamp AS DATE) AS trade_date,
                   AVG(price) AS avg_price
            FROM silver_trades GROUP BY symbol, CAST(timestamp AS DATE)
        )
        SELECT
            symbol,
            trade_date,
            avg_price,
            AVG(avg_price) OVER (
                PARTITION BY symbol ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS sma_20
        FROM daily
        ORDER BY symbol, trade_date
        LIMIT 50
    """).fetchall()
    assert len(rows) > 0


# ── Unusual volume detection ──────────────────────────────────────────────────

def test_unusual_volume_detection_sql(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        WITH per_day AS (
            SELECT symbol,
                   CAST(timestamp AS DATE) AS trade_date,
                   SUM(quantity) AS daily_volume
            FROM silver_trades
            GROUP BY symbol, CAST(timestamp AS DATE)
        ),
        with_avg AS (
            SELECT *,
                AVG(daily_volume) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                ) AS avg_vol,
                STDDEV(daily_volume) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
                ) AS std_vol
            FROM per_day
        )
        SELECT symbol, trade_date, daily_volume, avg_vol,
               CASE WHEN std_vol > 0
                    THEN (daily_volume - avg_vol) / std_vol
                    ELSE 0 END AS z_score
        FROM with_avg
        WHERE avg_vol IS NOT NULL
        ORDER BY z_score DESC NULLS LAST
        LIMIT 20
    """).fetchall()
    assert rows is not None  # may be empty for single-day data; must not error


# ── Sector rotation ────────────────────────────────────────────────────────────

def test_sector_rotation_sql(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT
            sector,
            SUM(price * quantity)         AS total_value,
            SUM(quantity)                 AS total_volume,
            COUNT(*)                      AS trade_count,
            SUM(CASE WHEN aggressor_side = 'Buy'  THEN quantity ELSE 0 END) AS buy_qty,
            SUM(CASE WHEN aggressor_side = 'Sell' THEN quantity ELSE 0 END) AS sell_qty
        FROM silver_trades
        GROUP BY sector
        ORDER BY total_value DESC
    """).fetchall()
    assert len(rows) > 0
    sectors = {r[0] for r in rows}
    assert "Technology" in sectors
    for row in rows:
        sector, val, vol, cnt, bq, sq = row
        assert val > 0
        assert vol > 0
        assert cnt > 0


# ── Daily market summary ──────────────────────────────────────────────────────

def test_daily_market_summary_sql(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT
            symbol,
            CAST(timestamp AS DATE)      AS trade_date,
            MIN(price)                   AS day_low,
            MAX(price)                   AS day_high,
            SUM(quantity)                AS total_volume,
            SUM(price * quantity)        AS total_value,
            COUNT(*)                     AS trade_count,
            SUM(quantity * price) / SUM(quantity) AS vwap
        FROM silver_trades
        GROUP BY symbol, CAST(timestamp AS DATE)
        ORDER BY trade_count DESC
        LIMIT 50
    """).fetchall()
    assert len(rows) > 0
    for row in rows:
        sym, date, low, high, vol, val, cnt, vwap = row
        assert low <= high, f"{sym}: low {low} > high {high}"
        assert vol > 0
        assert cnt > 0
        assert low <= vwap <= high, f"{sym}: VWAP {vwap} out of range [{low}, {high}]"


# ── Cross-account activity (wash-trade detection) ─────────────────────────────

def test_cross_account_activity_sql(pipeline_db, populated_pipeline):
    result = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades t1
        JOIN silver_trades t2
            ON  t1.symbol         = t2.symbol
            AND t1.aggressor_side != t2.aggressor_side
            AND CAST(t1.timestamp AS DATE) = CAST(t2.timestamp AS DATE)
            AND ABS(t1.quantity - t2.quantity) < t1.quantity * 0.05
            AND t1.aggressor_side = 'Buy'
    """).fetchone()[0]
    assert result >= 0  # must not error; may be 0 in random data


# ── Trader profile ────────────────────────────────────────────────────────────

def test_trader_profile_sql(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT
            buyer_agent_id                  AS agent_id,
            COUNT(*)                        AS trade_count,
            SUM(quantity)                   AS total_qty,
            SUM(price * quantity)           AS total_value,
            COUNT(DISTINCT symbol)          AS symbols_traded,
            MIN(CAST(timestamp AS DATE))    AS first_trade,
            MAX(CAST(timestamp AS DATE))    AS last_trade
        FROM silver_trades
        GROUP BY buyer_agent_id
        ORDER BY trade_count DESC
        LIMIT 20
    """).fetchall()
    assert len(rows) > 0
    for row in rows:
        agent, cnt, qty, val, syms, first, last = row
        assert cnt > 0
        assert qty > 0
        assert syms >= 1
        assert first <= last


# ── Market overview KPI SQL (as rendered by Superset) ─────────────────────────

def test_market_kpi_sql(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute("""
        SELECT
            SUM(total_volume)                             AS market_volume,
            ROUND(SUM(total_value), 2)                    AS market_value,
            SUM(trade_count)                              AS total_trades,
            COUNT(DISTINCT symbol)                        AS active_symbols,
            SUM(CASE WHEN close_price >= open_price THEN 1 ELSE 0 END) AS advances,
            SUM(CASE WHEN close_price < open_price  THEN 1 ELSE 0 END) AS declines
        FROM gold_daily_summary
    """).fetchone()
    market_vol, market_val, total_trades, syms, advances, declines = row
    assert market_vol > 0
    assert market_val > 0
    assert total_trades > 0
    assert syms == 6
    assert advances + declines == syms  # every symbol is either up or down
