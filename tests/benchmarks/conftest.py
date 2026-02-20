"""Benchmark fixtures — in-memory DuckDB with synthetic trade data at scale."""

import random
from datetime import date, datetime, timezone

import duckdb
import pytest


def _make_trades(n: int, symbols: list[str]) -> list[dict]:
    """Generate n synthetic trade rows."""
    rng = random.Random(42)
    base_ts = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        sym = symbols[i % len(symbols)]
        price = round(100.0 + rng.gauss(0, 5), 4)
        qty = rng.randint(1, 500)
        ts = base_ts.replace(second=base_ts.second + i % 60, microsecond=i * 1000 % 1_000_000)
        rows.append(
            {
                "trade_id": f"T{i:08d}",
                "symbol": sym,
                "price": price,
                "quantity": qty,
                "timestamp": ts,
                "company_name": f"Company {sym}",
                "sector": "Technology" if i % 2 == 0 else "Finance",
                "trading_date": date(2024, 1, 15),
            }
        )
    return rows


def _build_db(n_trades: int) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection populated with n_trades synthetic silver trades."""
    symbols = ["AAPL", "MSFT", "TSLA", "NVDA", "GOOGL", "AMZN"]
    rows = _make_trades(n_trades, symbols)

    con = duckdb.connect()
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '512MB'")
    con.execute("SET enable_object_cache = true")
    con.execute("""
        CREATE TABLE silver_trades (
            trade_id     VARCHAR,
            symbol       VARCHAR,
            price        DOUBLE,
            quantity     INTEGER,
            timestamp    TIMESTAMPTZ,
            company_name VARCHAR,
            sector       VARCHAR,
            trading_date DATE
        )
    """)
    con.executemany(
        "INSERT INTO silver_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r["trade_id"],
                r["symbol"],
                r["price"],
                r["quantity"],
                r["timestamp"],
                r["company_name"],
                r["sector"],
                r["trading_date"],
            )
            for r in rows
        ],
    )
    return con


@pytest.fixture(scope="session")
def bench_db_small() -> duckdb.DuckDBPyConnection:
    """10 000 trades — fast smoke-level benchmark."""
    con = _build_db(10_000)
    yield con
    con.close()


@pytest.fixture(scope="session")
def bench_db_medium() -> duckdb.DuckDBPyConnection:
    """100 000 trades — representative daily load."""
    con = _build_db(100_000)
    yield con
    con.close()


@pytest.fixture(scope="session")
def bench_db_large() -> duckdb.DuckDBPyConnection:
    """1 000 000 trades — stress / regression baseline."""
    con = _build_db(1_000_000)
    yield con
    con.close()
