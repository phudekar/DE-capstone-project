"""Tests for Gold layer asset definitions."""

from dagster import AssetKey

from orchestrator.assets.gold import (
    gold_daily_trading_summary,
    gold_market_overview,
    gold_portfolio_positions,
    gold_trader_performance,
)


def test_gold_daily_trading_summary_key():
    """Gold daily trading summary has correct key."""
    assert gold_daily_trading_summary.key == AssetKey("gold_daily_trading_summary")


def test_gold_daily_trading_summary_depends_on_silver():
    """Gold daily summary depends on silver_trades."""
    deps = gold_daily_trading_summary.asset_deps[AssetKey("gold_daily_trading_summary")]
    assert AssetKey("silver_trades") in deps


def test_gold_trader_performance_depends_on_silver():
    """Gold trader performance depends on silver_trades and silver_trader_activity."""
    deps = gold_trader_performance.asset_deps[AssetKey("gold_trader_performance")]
    assert AssetKey("silver_trades") in deps
    assert AssetKey("silver_trader_activity") in deps


def test_gold_market_overview_depends_on_silver():
    """Gold market overview depends on silver_trades and silver_market_data."""
    deps = gold_market_overview.asset_deps[AssetKey("gold_market_overview")]
    assert AssetKey("silver_trades") in deps
    assert AssetKey("silver_market_data") in deps


def test_gold_portfolio_positions_depends_on_silver():
    """Gold portfolio positions depends on silver_trades and silver_trader_activity."""
    deps = gold_portfolio_positions.asset_deps[AssetKey("gold_portfolio_positions")]
    assert AssetKey("silver_trades") in deps
    assert AssetKey("silver_trader_activity") in deps


def test_gold_daily_trading_summary_group():
    """Gold assets belong to the gold group."""
    assert gold_daily_trading_summary.group_names_by_key[AssetKey("gold_daily_trading_summary")] == "gold"


def test_gold_trader_performance_key():
    """Gold trader performance has correct key."""
    assert gold_trader_performance.key == AssetKey("gold_trader_performance")


def test_gold_market_overview_key():
    """Gold market overview has correct key."""
    assert gold_market_overview.key == AssetKey("gold_market_overview")


def test_gold_daily_trading_summary_is_partitioned():
    """Gold daily summary has a daily partition definition."""
    assert gold_daily_trading_summary.partitions_def is not None
