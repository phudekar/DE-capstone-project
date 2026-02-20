"""seed_test_data.py — Seed DuckDB with synthetic trade data for Superset testing.

Creates in-memory DuckDB tables that mirror the Iceberg views expected by Superset:
  - daily_trade_summary
  - trades_enriched
  - market_daily_overview
  - symbol_reference
  - sector_daily_summary
  - trader_performance_metrics
  - order_book_snapshots

Usage:
    python seed_test_data.py [--db-path PATH] [--days 7] [--symbols 10]
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NVDA", "JPM", "GS", "BAC"]
SECTORS = {
    "AAPL": "Technology", "MSFT": "Technology", "GOOG": "Technology",
    "AMZN": "Consumer Discretionary", "META": "Technology", "TSLA": "Consumer Discretionary",
    "NVDA": "Technology", "JPM": "Financials", "GS": "Financials", "BAC": "Financials",
}
BASE_PRICES = {s: round(random.uniform(50, 500), 2) for s in SYMBOLS}


def generate_trades(symbols: list[str], days: int) -> list[dict]:
    rows = []
    today = date.today()
    for d in range(days):
        trade_date = today - timedelta(days=days - d - 1)
        for symbol in symbols:
            price = BASE_PRICES[symbol]
            n_trades = random.randint(20, 100)
            for i in range(n_trades):
                price *= random.uniform(0.995, 1.005)
                qty = random.randint(10, 500)
                rows.append({
                    "trade_id": f"T-{symbol}-{trade_date}-{i:04d}",
                    "symbol": symbol,
                    "trade_type": random.choice(["BUY", "SELL"]),
                    "price": round(price, 4),
                    "quantity": qty,
                    "value": round(price * qty, 2),
                    "traded_at": f"{trade_date}T{9 + i // 10:02d}:{i % 60:02d}:00",
                    "trade_date": str(trade_date),
                    "account_id": f"ACC-{random.randint(1000, 9999)}",
                    "sector": SECTORS.get(symbol, "Other"),
                })
        BASE_PRICES[symbol] = round(price, 2)
    return rows


def seed(db_path: str, days: int, n_symbols: int) -> None:
    symbols = SYMBOLS[:n_symbols]
    trades = generate_trades(symbols, days)
    print(f"Generated {len(trades)} trade rows for {n_symbols} symbols over {days} days.")

    conn = duckdb.connect(db_path)

    # trades_enriched
    conn.execute("DROP TABLE IF EXISTS trades_enriched")
    conn.execute("""
        CREATE TABLE trades_enriched (
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
    conn.executemany("""
        INSERT INTO trades_enriched VALUES (
            ?, ?, ?, ?, ?, ?, ?::TIMESTAMP, ?::DATE, ?, ?
        )
    """, [
        (r["trade_id"], r["symbol"], r["trade_type"], r["price"],
         r["quantity"], r["value"], r["traded_at"],
         r["trade_date"], r["account_id"], r["sector"])
        for r in trades
    ])

    # daily_trade_summary (aggregated from trades_enriched)
    conn.execute("DROP VIEW IF EXISTS daily_trade_summary")
    conn.execute("""
        CREATE VIEW daily_trade_summary AS
        SELECT
            trade_date,
            symbol,
            COUNT(*)            AS trade_count,
            SUM(quantity)       AS total_volume,
            SUM(value)          AS total_value,
            MIN(price)          AS low_price,
            MAX(price)          AS high_price,
            AVG(price)          AS avg_price,
            sector
        FROM trades_enriched
        GROUP BY trade_date, symbol, sector
    """)

    # symbol_reference
    conn.execute("DROP TABLE IF EXISTS symbol_reference")
    conn.execute("""
        CREATE TABLE symbol_reference (
            symbol      VARCHAR PRIMARY KEY,
            company     VARCHAR,
            sector      VARCHAR,
            market_cap  DOUBLE
        )
    """)
    conn.executemany("INSERT INTO symbol_reference VALUES (?, ?, ?, ?)", [
        (s, f"{s} Corp", SECTORS.get(s, "Other"), round(random.uniform(1e9, 3e12), 0))
        for s in symbols
    ])

    # market_daily_overview
    conn.execute("DROP VIEW IF EXISTS market_daily_overview")
    conn.execute("""
        CREATE VIEW market_daily_overview AS
        SELECT
            trade_date,
            SUM(total_volume)   AS market_volume,
            SUM(total_value)    AS market_value,
            COUNT(DISTINCT symbol) AS active_symbols,
            SUM(trade_count)    AS total_trades
        FROM daily_trade_summary
        GROUP BY trade_date
    """)

    # sector_daily_summary
    conn.execute("DROP VIEW IF EXISTS sector_daily_summary")
    conn.execute("""
        CREATE VIEW sector_daily_summary AS
        SELECT
            trade_date,
            sector,
            COUNT(DISTINCT symbol)  AS symbol_count,
            SUM(total_volume)       AS total_volume,
            SUM(total_value)        AS total_value,
            AVG(avg_price)          AS avg_price
        FROM daily_trade_summary
        GROUP BY trade_date, sector
    """)

    # trader_performance_metrics
    conn.execute("DROP VIEW IF EXISTS trader_performance_metrics")
    conn.execute("""
        CREATE VIEW trader_performance_metrics AS
        SELECT
            account_id,
            COUNT(*)                AS trade_count,
            SUM(CASE WHEN trade_type='BUY'  THEN quantity ELSE 0 END) AS bought_qty,
            SUM(CASE WHEN trade_type='SELL' THEN quantity ELSE 0 END) AS sold_qty,
            SUM(value)              AS total_value,
            COUNT(DISTINCT symbol)  AS symbols_traded,
            MIN(trade_date)         AS first_trade_date,
            MAX(trade_date)         AS last_trade_date
        FROM trades_enriched
        GROUP BY account_id
    """)

    conn.close()
    print(f"Seeded DuckDB at: {db_path}")
    print("Tables/views created: trades_enriched, daily_trade_summary, symbol_reference,")
    print("  market_daily_overview, sector_daily_summary, trader_performance_metrics")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed test DuckDB for Superset.")
    parser.add_argument("--db-path", default=":memory:",
                        help="DuckDB file path (default: :memory:, prints summary only)")
    parser.add_argument("--days", type=int, default=7, help="Days of trade history (default: 7)")
    parser.add_argument("--symbols", type=int, default=len(SYMBOLS),
                        help=f"Number of symbols to include (max {len(SYMBOLS)}, default: all)")
    args = parser.parse_args()

    if args.db_path == ":memory:":
        print("NOTE: Using :memory: — data will not be persisted. Pass --db-path to save.")

    seed(args.db_path, args.days, min(args.symbols, len(SYMBOLS)))


if __name__ == "__main__":
    main()
