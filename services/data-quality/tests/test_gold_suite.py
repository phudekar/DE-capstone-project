"""Tests for gold expectation suite."""

import datetime

import pandas as pd

from data_quality.runner import validate_dataframe
from data_quality.suites.gold_suite import build_gold_daily_summary_suite

# Register the custom expectation so GX recognizes it
import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401


class TestGoldSuite:
    def test_valid_data_passes(self, valid_gold_df):
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(valid_gold_df, suite)
        assert result["success"] is True

    def test_null_symbol_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "symbol"] = None
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_zero_trade_count_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "trade_count"] = 0
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_values_to_be_between" for r in failed)

    def test_zero_volume_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "total_volume"] = 0
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert len(failed) > 0

    def test_high_below_open_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "high_price"] = 140.00  # Below open_price of 150.00
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(
            r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
            for r in failed
        )

    def test_low_above_close_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "low_price"] = 160.00  # Above close_price of 155.00
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(
            r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
            for r in failed
        )

    def test_vwap_outside_range_fails(self, valid_gold_df):
        df = valid_gold_df.copy()
        df.loc[0, "vwap"] = 200.00  # Above high_price of 158.00
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(
            r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
            for r in failed
        )

    def test_missing_column_fails(self, valid_gold_df):
        df = valid_gold_df.drop(columns=["vwap"])
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        failed = [r for r in result["results"] if not r["success"]]
        assert any(r["expectation_type"] == "expect_column_to_exist" for r in failed)
