"""DuckDB query performance benchmarks.

These tests assert that key analytical queries complete within acceptable
wall-clock thresholds.  They run without any Docker services.

Mark: ``pytest -m benchmark`` or ``make benchmark``.
"""

import time

import pytest

pytestmark = pytest.mark.benchmark

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

VWAP_SQL = """
    SELECT
        symbol,
        CASE WHEN SUM(quantity) > 0
            THEN SUM(price * quantity) / SUM(quantity)
            ELSE 0.0
        END AS vwap,
        SUM(quantity)   AS total_volume,
        COUNT(*)        AS trade_count,
        MIN(price)      AS low_price,
        MAX(price)      AS high_price
    FROM silver_trades
    GROUP BY symbol
    ORDER BY symbol
"""

OHLCV_SQL = """
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp ASC)  AS rn_first,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn_last
        FROM silver_trades
    )
    SELECT
        symbol,
        MIN(CASE WHEN rn_first = 1 THEN price END) AS open_price,
        MIN(CASE WHEN rn_last  = 1 THEN price END) AS close_price,
        MAX(price)      AS high_price,
        MIN(price)      AS low_price,
        SUM(quantity)   AS total_volume,
        COUNT(*)        AS trade_count
    FROM ranked
    GROUP BY symbol
    ORDER BY symbol
"""

SECTOR_SQL = """
    SELECT
        sector,
        COUNT(DISTINCT symbol) AS symbol_count,
        SUM(quantity)          AS total_volume,
        SUM(price * quantity)  AS total_value
    FROM silver_trades
    GROUP BY sector
    ORDER BY total_value DESC
"""

TOP_TRADES_SQL = """
    SELECT trade_id, symbol, price, quantity, price * quantity AS notional
    FROM silver_trades
    ORDER BY notional DESC
    LIMIT 10
"""


def _run_timed(con, sql: str) -> float:
    """Execute *sql* and return elapsed seconds."""
    t0 = time.perf_counter()
    con.execute(sql).fetchall()
    return time.perf_counter() - t0


# ──────────────────────────────────────────────────────────────────────────────
# 10 k rows
# ──────────────────────────────────────────────────────────────────────────────


class TestSmallDataset:
    """10 000-row benchmarks — must all finish in < 0.5 s."""

    def test_vwap_small(self, bench_db_small):
        elapsed = _run_timed(bench_db_small, VWAP_SQL)
        assert elapsed < 0.5, f"VWAP on 10k rows took {elapsed:.3f}s (threshold 0.5s)"

    def test_ohlcv_small(self, bench_db_small):
        elapsed = _run_timed(bench_db_small, OHLCV_SQL)
        assert elapsed < 0.5, f"OHLCV on 10k rows took {elapsed:.3f}s (threshold 0.5s)"

    def test_sector_aggregation_small(self, bench_db_small):
        elapsed = _run_timed(bench_db_small, SECTOR_SQL)
        assert elapsed < 0.5, f"Sector agg on 10k rows took {elapsed:.3f}s (threshold 0.5s)"

    def test_top_trades_small(self, bench_db_small):
        elapsed = _run_timed(bench_db_small, TOP_TRADES_SQL)
        assert elapsed < 0.5, f"Top-trades on 10k rows took {elapsed:.3f}s (threshold 0.5s)"


# ──────────────────────────────────────────────────────────────────────────────
# 100 k rows
# ──────────────────────────────────────────────────────────────────────────────


class TestMediumDataset:
    """100 000-row benchmarks — must all finish in < 2 s."""

    def test_vwap_medium(self, bench_db_medium):
        elapsed = _run_timed(bench_db_medium, VWAP_SQL)
        assert elapsed < 2.0, f"VWAP on 100k rows took {elapsed:.3f}s (threshold 2.0s)"

    def test_ohlcv_medium(self, bench_db_medium):
        elapsed = _run_timed(bench_db_medium, OHLCV_SQL)
        assert elapsed < 2.0, f"OHLCV on 100k rows took {elapsed:.3f}s (threshold 2.0s)"

    def test_sector_aggregation_medium(self, bench_db_medium):
        elapsed = _run_timed(bench_db_medium, SECTOR_SQL)
        assert elapsed < 2.0, f"Sector agg on 100k rows took {elapsed:.3f}s (threshold 2.0s)"

    def test_top_trades_medium(self, bench_db_medium):
        elapsed = _run_timed(bench_db_medium, TOP_TRADES_SQL)
        assert elapsed < 2.0, f"Top-trades on 100k rows took {elapsed:.3f}s (threshold 2.0s)"


# ──────────────────────────────────────────────────────────────────────────────
# 1 M rows  (labelled "slow" — only run when explicitly requested)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.slow
class TestLargeDataset:
    """1 000 000-row benchmarks — must all finish in < 10 s."""

    def test_vwap_large(self, bench_db_large):
        elapsed = _run_timed(bench_db_large, VWAP_SQL)
        assert elapsed < 10.0, f"VWAP on 1M rows took {elapsed:.3f}s (threshold 10.0s)"

    def test_ohlcv_large(self, bench_db_large):
        elapsed = _run_timed(bench_db_large, OHLCV_SQL)
        assert elapsed < 10.0, f"OHLCV on 1M rows took {elapsed:.3f}s (threshold 10.0s)"

    def test_sector_aggregation_large(self, bench_db_large):
        elapsed = _run_timed(bench_db_large, SECTOR_SQL)
        assert elapsed < 10.0, f"Sector agg on 1M rows took {elapsed:.3f}s (threshold 10.0s)"

    def test_top_trades_large(self, bench_db_large):
        elapsed = _run_timed(bench_db_large, TOP_TRADES_SQL)
        assert elapsed < 10.0, f"Top-trades on 1M rows took {elapsed:.3f}s (threshold 10.0s)"
