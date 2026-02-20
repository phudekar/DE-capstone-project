"""Tests for silver expectation suite."""

from data_quality.runner import validate_dataframe
from data_quality.suites.silver_suite import build_silver_trades_suite


class TestSilverSuite:
    def test_valid_data_passes(self, valid_silver_df):
        suite = build_silver_trades_suite()
        result = validate_dataframe(valid_silver_df, suite)
        assert result["success"] is True

    def test_duplicate_trade_id_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[2, "trade_id"] = df.loc[0, "trade_id"]  # Duplicate
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_unique" for r in failed)

    def test_null_trade_id_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[0, "trade_id"] = None
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_price_too_high_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[0, "price"] = 200000.0
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_between" for r in failed)

    def test_price_too_low_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[0, "price"] = 0.001
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_quantity_too_high_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[0, "quantity"] = 2000000
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_unknown_symbol_fails(self, valid_silver_df):
        df = valid_silver_df.copy()
        df.loc[0, "symbol"] = "UNKNOWN"
        suite = build_silver_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_in_set" for r in failed)

    def test_custom_symbols(self, valid_silver_df):
        df = valid_silver_df.copy()
        df["symbol"] = ["XYZ", "XYZ", "XYZ"]
        suite = build_silver_trades_suite(symbols=["XYZ"])
        result = validate_dataframe(df, suite)
        # Should pass with custom symbol set
        symbol_results = [
            r for r in result["results"]
            if r["expectation_type"] == "expect_column_values_to_be_in_set"
        ]
        assert all(r["success"] for r in symbol_results)
