"""Tests for bronze expectation suite."""

import pandas as pd

from data_quality.runner import validate_dataframe
from data_quality.suites.bronze_suite import build_bronze_trades_suite


class TestBronzeSuite:
    def test_valid_data_passes(self, valid_bronze_df):
        suite = build_bronze_trades_suite()
        result = validate_dataframe(valid_bronze_df, suite)
        assert result["success"] is True

    def test_null_trade_id_fails(self, valid_bronze_df):
        df = valid_bronze_df.copy()
        df.loc[0, "trade_id"] = None
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_negative_price_fails(self, valid_bronze_df):
        df = valid_bronze_df.copy()
        df.loc[0, "price"] = -10.0
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_between" for r in failed)

    def test_zero_price_fails(self, valid_bronze_df):
        df = valid_bronze_df.copy()
        df.loc[0, "price"] = 0.0
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_zero_quantity_fails(self, valid_bronze_df):
        df = valid_bronze_df.copy()
        df.loc[0, "quantity"] = 0
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_invalid_aggressor_side_fails(self, valid_bronze_df):
        df = valid_bronze_df.copy()
        df.loc[0, "aggressor_side"] = "Invalid"
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_in_set" for r in failed)

    def test_missing_column_fails(self, valid_bronze_df):
        df = valid_bronze_df.drop(columns=["symbol"])
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_to_exist" for r in failed)

    def test_empty_table_fails(self):
        df = pd.DataFrame(
            columns=[
                "trade_id", "symbol", "price", "quantity", "buyer_order_id",
                "seller_order_id", "buyer_agent_id", "seller_agent_id",
                "aggressor_side", "event_type", "timestamp",
                "_kafka_topic", "_kafka_partition", "_kafka_offset", "_ingested_at",
            ]
        )
        suite = build_bronze_trades_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(
            r["expectation_type"] == "expect_table_row_count_to_be_between" for r in failed
        )
