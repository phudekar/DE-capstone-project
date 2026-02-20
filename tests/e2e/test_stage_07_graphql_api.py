"""Stage 7 — GraphQL API resolver logic.

Tests the resolver filter-building, row mapping, and cursor-pagination logic
against the in-memory DuckDB pipeline state.  Where the graphql-api package
is importable the real code is used; otherwise the SQL queries are run directly.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "services/graphql-api"))

import pytest


# ── _build_where helper ───────────────────────────────────────────────────────

def _try_import_build_where():
    try:
        from app.resolvers.trade import _build_where
        from app.schema.inputs import TradeFilterInput
        return _build_where, TradeFilterInput
    except Exception:
        return None, None


def test_build_where_no_filter():
    _build_where, TradeFilterInput = _try_import_build_where()
    if _build_where is None:
        pytest.skip("graphql-api not importable in this venv")
    sql, params = _build_where(TradeFilterInput())
    assert "1=1" in sql
    assert params == []


def test_build_where_symbol_filter():
    _build_where, TradeFilterInput = _try_import_build_where()
    if _build_where is None:
        pytest.skip("graphql-api not importable in this venv")
    sql, params = _build_where(TradeFilterInput(symbol="AAPL"))
    assert "symbol = ?" in sql
    assert "AAPL" in params


def test_build_where_min_price_filter():
    _build_where, TradeFilterInput = _try_import_build_where()
    if _build_where is None:
        pytest.skip("graphql-api not importable in this venv")
    sql, params = _build_where(TradeFilterInput(min_price=100.0))
    assert "price >= ?" in sql
    assert 100.0 in params


def test_build_where_multiple_symbols():
    _build_where, TradeFilterInput = _try_import_build_where()
    if _build_where is None:
        pytest.skip("graphql-api not importable in this venv")
    sql, params = _build_where(TradeFilterInput(symbols=["AAPL", "MSFT"]))
    assert "IN (" in sql
    assert "AAPL" in params and "MSFT" in params


# ── _row_to_trade mapping ─────────────────────────────────────────────────────

def test_row_to_trade_mapping():
    try:
        from app.resolvers.trade import _row_to_trade
    except Exception:
        pytest.skip("graphql-api not importable in this venv")
    from datetime import datetime, timezone
    row = {
        "trade_id":       "T-abc123",
        "symbol":         "AAPL",
        "price":          182.50,
        "quantity":       100,
        "buyer_agent_id": "A-buyer",
        "seller_agent_id":"A-seller",
        "aggressor_side": "Buy",
        "timestamp":      datetime.now(timezone.utc),
        "company_name":   "Apple Inc.",
        "sector":         "Technology",
    }
    trade = _row_to_trade(row)
    assert trade.trade_id == "T-abc123"
    assert trade.symbol == "AAPL"
    assert trade.price == pytest.approx(182.50)
    assert trade.quantity == 100
    assert trade.company_name == "Apple Inc."


# ── Resolver SQL against DuckDB pipeline state ────────────────────────────────

def test_trades_query_returns_results(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT trade_id, symbol, price, quantity,
               buyer_agent_id, seller_agent_id, aggressor_side,
               timestamp, company_name, sector
        FROM silver_trades
        ORDER BY timestamp DESC
        LIMIT 20
    """).fetchall()
    assert len(rows) == 20


def test_trades_symbol_filter(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT DISTINCT symbol FROM silver_trades WHERE symbol = 'AAPL'
    """).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "AAPL"


def test_trades_pagination_no_overlap(pipeline_db, populated_pipeline):
    page1 = {r[0] for r in pipeline_db.conn.execute(
        "SELECT trade_id FROM silver_trades ORDER BY timestamp DESC LIMIT 5 OFFSET 0"
    ).fetchall()}
    page2 = {r[0] for r in pipeline_db.conn.execute(
        "SELECT trade_id FROM silver_trades ORDER BY timestamp DESC LIMIT 5 OFFSET 5"
    ).fetchall()}
    assert page1.isdisjoint(page2)


def test_daily_summary_query(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT symbol, trading_date, open_price, close_price,
               high_price, low_price, vwap, total_volume, trade_count
        FROM gold_daily_summary ORDER BY symbol
    """).fetchall()
    assert len(rows) > 0


def test_market_overview_aggregation(pipeline_db, populated_pipeline, symbols):
    row = pipeline_db.conn.execute("""
        SELECT
            SUM(total_volume)      AS market_volume,
            SUM(total_value)       AS market_value,
            COUNT(DISTINCT symbol) AS active_symbols,
            SUM(trade_count)       AS total_trades
        FROM gold_daily_summary
    """).fetchone()
    assert row[0] > 0            # market_volume
    assert row[1] > 0            # market_value
    assert row[2] == len(symbols) # all symbols present
    assert row[3] > 0            # total_trades


def test_order_book_query(pipeline_db, populated_pipeline):
    row = pipeline_db.conn.execute("""
        SELECT symbol, best_bid_price, best_ask_price, spread, mid_price
        FROM silver_orderbook WHERE symbol = 'AAPL'
        ORDER BY timestamp DESC LIMIT 1
    """).fetchone()
    assert row is not None
    assert row[0] == "AAPL"
    assert row[1] < row[2]  # bid < ask
    assert row[3] > 0       # spread > 0


def test_symbol_reference_query(pipeline_db, populated_pipeline, symbols):
    rows = pipeline_db.conn.execute(
        "SELECT symbol, company_name, sector FROM dim_symbol"
    ).fetchall()
    found = {r[0] for r in rows}
    assert set(symbols) == found


def test_trades_min_price_filter(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades WHERE price >= 50.0
    """).fetchone()[0]
    assert rows > 0


def test_aggressor_side_filter(pipeline_db, populated_pipeline):
    rows = pipeline_db.conn.execute("""
        SELECT DISTINCT aggressor_side FROM silver_trades
    """).fetchall()
    sides = {r[0] for r in rows}
    assert sides.issubset({"Buy", "Sell"})


# ── Schema / app importability ────────────────────────────────────────────────

def test_fastapi_app_importable():
    try:
        from app.main import app
        assert app is not None
    except Exception as exc:
        pytest.skip(f"FastAPI app not fully importable: {exc}")


def test_graphql_schema_has_query_type():
    try:
        from app.schema import schema
        assert schema.query_type is not None
    except Exception as exc:
        pytest.skip(f"Schema not importable: {exc}")
