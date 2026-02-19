"""Tests for Gold aggregation logic using DuckDB."""

from datetime import date, datetime, timezone

import duckdb
import pyarrow as pa


class TestGoldAggregation:
    """Test the DuckDB aggregation SQL used in gold_aggregator."""

    def _make_silver_trades(self) -> pa.Table:
        """Create sample Silver trades for aggregation testing."""
        return pa.table(
            {
                "trade_id": ["T-001", "T-002", "T-003", "T-004"],
                "symbol": ["AAPL", "AAPL", "AAPL", "GOOGL"],
                "price": [180.0, 185.0, 182.0, 2800.0],
                "quantity": [100, 200, 150, 50],
                "timestamp": [
                    datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, 15, 30, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, 11, 0, tzinfo=timezone.utc),
                ],
                "company_name": ["Apple Inc.", "Apple Inc.", "Apple Inc.", "Alphabet Inc."],
                "sector": ["Technology", "Technology", "Technology", "Technology"],
            }
        )

    def test_daily_aggregation_ohlc(self):
        silver = self._make_silver_trades()
        con = duckdb.connect()
        con.register("silver_trades", silver)
        trading_date = date(2024, 1, 15)

        result = con.execute(
            """
            SELECT
                symbol,
                first(price ORDER BY timestamp ASC) AS open_price,
                last(price ORDER BY timestamp ASC) AS close_price,
                max(price) AS high_price,
                min(price) AS low_price,
                CASE WHEN sum(quantity) > 0
                    THEN sum(price * quantity) / sum(quantity)
                    ELSE 0.0
                END AS vwap,
                CAST(sum(quantity) AS BIGINT) AS total_volume,
                CAST(count(*) AS INTEGER) AS trade_count,
                sum(price * quantity) AS total_value,
            FROM silver_trades
            WHERE CAST(timestamp AS DATE) = ?
            GROUP BY symbol
            ORDER BY symbol
            """,
            [trading_date],
        ).fetch_arrow_table()
        con.close()

        assert len(result) == 2  # AAPL + GOOGL

        # Check AAPL
        aapl = result.filter(pa.compute.equal(result.column("symbol"), "AAPL"))
        assert aapl.column("open_price")[0].as_py() == 180.0   # First trade
        assert aapl.column("close_price")[0].as_py() == 182.0  # Last trade
        assert aapl.column("high_price")[0].as_py() == 185.0
        assert aapl.column("low_price")[0].as_py() == 180.0
        assert aapl.column("total_volume")[0].as_py() == 450
        assert aapl.column("trade_count")[0].as_py() == 3

    def test_vwap_computation(self):
        """VWAP = sum(price * qty) / sum(qty)."""
        silver = self._make_silver_trades()
        con = duckdb.connect()
        con.register("silver_trades", silver)
        trading_date = date(2024, 1, 15)

        result = con.execute(
            """
            SELECT symbol,
                   sum(price * quantity) / sum(quantity) AS vwap
            FROM silver_trades
            WHERE symbol = 'AAPL' AND CAST(timestamp AS DATE) = ?
            GROUP BY symbol
            """,
            [trading_date],
        ).fetchone()
        con.close()

        # VWAP = (180*100 + 185*200 + 182*150) / (100+200+150) = 82300/450 ≈ 182.89
        expected_vwap = (180.0 * 100 + 185.0 * 200 + 182.0 * 150) / 450
        assert abs(result[1] - expected_vwap) < 0.01

    def test_empty_result_for_wrong_date(self):
        silver = self._make_silver_trades()
        con = duckdb.connect()
        con.register("silver_trades", silver)

        result = con.execute(
            """
            SELECT symbol, count(*) AS cnt
            FROM silver_trades
            WHERE CAST(timestamp AS DATE) = ?
            GROUP BY symbol
            """,
            [date(2099, 1, 1)],
        ).fetch_arrow_table()
        con.close()

        assert len(result) == 0
