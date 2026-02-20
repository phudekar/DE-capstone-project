"""Shared pytest fixtures for Superset layer tests.

All tests run against an in-memory DuckDB database seeded with synthetic
trade data, without requiring a live Superset or Iceberg cluster.
"""

import random
from datetime import date, timedelta

import duckdb
import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
SECTORS = {
    "AAPL": "Technology",
    "MSFT": "Technology",
    "GOOG": "Technology",
    "AMZN": "Consumer Discretionary",
    "META": "Technology",
}
ACCOUNTS = [f"ACC-{i:04d}" for i in range(1, 21)]
TRADE_DAYS = 7


def _seed_conn(conn: duckdb.DuckDBPyConnection) -> None:
    """Populate an existing DuckDB connection with test tables and views."""
    today = date.today()
    random.seed(42)

    # ── trades_enriched ───────────────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE TABLE trades_enriched (
            trade_id    VARCHAR,
            symbol      VARCHAR,
            trade_type  VARCHAR,
            price       DOUBLE,
            quantity    INTEGER,
            value       DOUBLE,
            traded_at   TIMESTAMP,
            trade_date  DATE,
            account_id  VARCHAR,
            sector      VARCHAR
        )
    """)

    rows = []
    for d in range(TRADE_DAYS):
        td = today - timedelta(days=TRADE_DAYS - d - 1)
        for sym in SYMBOLS:
            price = random.uniform(50.0, 500.0)
            for i in range(30):
                price *= random.uniform(0.998, 1.002)
                qty = random.randint(10, 200)
                rows.append((
                    f"T-{sym}-{td}-{i:04d}",
                    sym,
                    random.choice(["BUY", "SELL"]),
                    round(price, 4),
                    qty,
                    round(price * qty, 2),
                    f"{td}T{9 + i // 10:02d}:{i % 60:02d}:00",
                    str(td),
                    random.choice(ACCOUNTS),
                    SECTORS[sym],
                ))

    conn.executemany("""
        INSERT INTO trades_enriched VALUES (
            ?, ?, ?, ?, ?, ?, ?::TIMESTAMP, ?::DATE, ?, ?
        )
    """, rows)

    # ── daily_trade_summary (view) ────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE VIEW daily_trade_summary AS
        SELECT
            trade_date,
            symbol,
            COUNT(*)          AS trade_count,
            SUM(quantity)     AS total_volume,
            SUM(value)        AS total_value,
            MIN(price)        AS low_price,
            MAX(price)        AS high_price,
            AVG(price)        AS avg_price,
            sector
        FROM trades_enriched
        GROUP BY trade_date, symbol, sector
    """)

    # ── symbol_reference ──────────────────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE TABLE symbol_reference (
            symbol      VARCHAR,
            company     VARCHAR,
            sector      VARCHAR,
            market_cap  DOUBLE
        )
    """)
    conn.executemany("INSERT INTO symbol_reference VALUES (?, ?, ?, ?)", [
        (s, f"{s} Corp", SECTORS[s], round(random.uniform(1e9, 3e12), 0))
        for s in SYMBOLS
    ])

    # ── market_daily_overview (view) ──────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE VIEW market_daily_overview AS
        SELECT
            trade_date,
            SUM(total_volume)      AS market_volume,
            SUM(total_value)       AS market_value,
            COUNT(DISTINCT symbol) AS active_symbols,
            SUM(trade_count)       AS total_trades
        FROM daily_trade_summary
        GROUP BY trade_date
    """)

    # ── sector_daily_summary (view) ───────────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE VIEW sector_daily_summary AS
        SELECT
            trade_date,
            sector,
            COUNT(DISTINCT symbol) AS symbol_count,
            SUM(total_volume)      AS total_volume,
            SUM(total_value)       AS total_value,
            AVG(avg_price)         AS avg_price
        FROM daily_trade_summary
        GROUP BY trade_date, sector
    """)

    # ── trader_performance_metrics (view) ─────────────────────────────────────
    conn.execute("""
        CREATE OR REPLACE VIEW trader_performance_metrics AS
        SELECT
            account_id,
            COUNT(*)              AS trade_count,
            SUM(CASE WHEN trade_type = 'BUY'  THEN quantity ELSE 0 END) AS bought_qty,
            SUM(CASE WHEN trade_type = 'SELL' THEN quantity ELSE 0 END) AS sold_qty,
            SUM(value)            AS total_value,
            COUNT(DISTINCT symbol) AS symbols_traded,
            MIN(trade_date)       AS first_trade_date,
            MAX(trade_date)       AS last_trade_date
        FROM trades_enriched
        GROUP BY account_id
    """)


@pytest.fixture(scope="session")
def duckdb_conn():
    """Session-scoped in-memory DuckDB with trade data pre-seeded."""
    conn = duckdb.connect(":memory:")
    _seed_conn(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def today() -> date:
    return date.today()


@pytest.fixture(scope="session")
def symbols() -> list[str]:
    return SYMBOLS


@pytest.fixture(scope="session")
def accounts() -> list[str]:
    return ACCOUNTS
