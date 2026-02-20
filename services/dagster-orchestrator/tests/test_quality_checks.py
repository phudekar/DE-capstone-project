"""Tests for GX asset check definitions."""

from orchestrator.assets.quality_checks import (
    bronze_raw_trades_quality_check,
    gold_daily_summary_quality_check,
    silver_trades_quality_check,
)


def _first_spec(check_def):
    return list(check_def.check_specs)[0]


def test_bronze_quality_check_defined():
    """bronze_raw_trades_quality_check is defined with the correct name."""
    spec = _first_spec(bronze_raw_trades_quality_check)
    assert spec.name == "bronze_raw_trades_quality_check"


def test_silver_quality_check_defined():
    """silver_trades_quality_check is defined with the correct name."""
    spec = _first_spec(silver_trades_quality_check)
    assert spec.name == "silver_trades_quality_check"


def test_gold_quality_check_defined():
    """gold_daily_summary_quality_check is defined with the correct name."""
    spec = _first_spec(gold_daily_summary_quality_check)
    assert spec.name == "gold_daily_summary_quality_check"


def test_bronze_check_targets_correct_asset():
    """bronze check targets the bronze_raw_trades asset."""
    spec = _first_spec(bronze_raw_trades_quality_check)
    assert spec.asset_key.path == ["bronze_raw_trades"]


def test_silver_check_targets_correct_asset():
    """silver check targets the silver_trades asset."""
    spec = _first_spec(silver_trades_quality_check)
    assert spec.asset_key.path == ["silver_trades"]


def test_gold_check_targets_correct_asset():
    """gold check targets the gold_daily_trading_summary asset."""
    spec = _first_spec(gold_daily_summary_quality_check)
    assert spec.asset_key.path == ["gold_daily_trading_summary"]
