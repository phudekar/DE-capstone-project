"""Tests for custom OHLCV consistency expectation."""

import datetime

import pandas as pd
import pytest

# Register the custom expectation before importing the suite
import data_quality.custom_expectations.expect_ohlcv_consistency  # noqa: F401
from data_quality.runner import validate_dataframe
from data_quality.suites.gold_suite import build_gold_daily_summary_suite


def _make_gold_df(**overrides):
    now = pd.Timestamp.now(tz="UTC")
    today = datetime.date.today()
    data = {
        "symbol": ["AAPL"],
        "trading_date": [today],
        "open_price": [150.00],
        "close_price": [155.00],
        "high_price": [160.00],
        "low_price": [145.00],
        "vwap": [152.00],
        "total_volume": [10000],
        "trade_count": [500],
        "total_value": [1520000.0],
        "company_name": ["Apple Inc."],
        "sector": ["Technology"],
        "_aggregated_at": [now],
    }
    data.update(overrides)
    return pd.DataFrame(data)


class TestOhlcvConsistency:
    def test_valid_ohlcv_passes(self):
        """A well-formed OHLCV row passes the consistency check."""
        df = _make_gold_df()
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        ohlcv_results = [
            r for r in result["results"]
            if r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
        ]
        assert len(ohlcv_results) == 1
        assert ohlcv_results[0]["success"] is True

    def test_high_below_open_fails(self):
        """high_price < open_price violates the OHLCV rule."""
        df = _make_gold_df(high_price=[140.00])  # Below open 150
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        ohlcv_results = [
            r for r in result["results"]
            if r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
        ]
        assert ohlcv_results[0]["success"] is False

    def test_low_above_close_fails(self):
        """low_price > close_price violates the OHLCV rule."""
        df = _make_gold_df(low_price=[165.00])  # Above close 155
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        ohlcv_results = [
            r for r in result["results"]
            if r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
        ]
        assert ohlcv_results[0]["success"] is False

    def test_vwap_above_high_fails(self):
        """vwap > high_price violates the OHLCV rule."""
        df = _make_gold_df(vwap=[170.00])  # Above high 160
        suite = build_gold_daily_summary_suite()
        result = validate_dataframe(df, suite)
        ohlcv_results = [
            r for r in result["results"]
            if r["expectation_type"] == "expect_multicolumn_values_to_be_ohlcv_consistent"
        ]
        assert ohlcv_results[0]["success"] is False
