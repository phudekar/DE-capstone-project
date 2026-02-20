"""Tests for Silver layer asset definitions."""

from dagster import AssetKey

from orchestrator.assets.silver import (
    silver_market_data,
    silver_orderbook_snapshots,
    silver_trades,
    silver_trader_activity,
)


def test_silver_trades_asset_key():
    """Silver trades asset has correct key."""
    assert silver_trades.key == AssetKey("silver_trades")


def test_silver_orderbook_snapshots_asset_key():
    """Silver orderbook snapshots asset has correct key."""
    assert silver_orderbook_snapshots.key == AssetKey("silver_orderbook_snapshots")


def test_silver_trades_depends_on_bronze():
    """Silver trades depends on bronze_raw_trades."""
    deps = silver_trades.asset_deps[AssetKey("silver_trades")]
    assert AssetKey("bronze_raw_trades") in deps


def test_silver_orderbook_depends_on_bronze():
    """Silver orderbook depends on bronze_raw_orderbook."""
    deps = silver_orderbook_snapshots.asset_deps[AssetKey("silver_orderbook_snapshots")]
    assert AssetKey("bronze_raw_orderbook") in deps


def test_silver_market_data_depends_on_bronze():
    """Silver market data depends on bronze_raw_marketdata."""
    deps = silver_market_data.asset_deps[AssetKey("silver_market_data")]
    assert AssetKey("bronze_raw_marketdata") in deps


def test_silver_trader_activity_depends_on_bronze():
    """Silver trader activity depends on bronze_raw_trades."""
    deps = silver_trader_activity.asset_deps[AssetKey("silver_trader_activity")]
    assert AssetKey("bronze_raw_trades") in deps


def test_silver_trades_group():
    """Silver assets belong to the silver group."""
    assert silver_trades.group_names_by_key[AssetKey("silver_trades")] == "silver"


def test_silver_trades_is_partitioned():
    """Silver trades has a daily partition definition."""
    assert silver_trades.partitions_def is not None
