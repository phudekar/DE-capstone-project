"""Tests for Bronze layer asset definitions."""

from dagster import AssetKey

from orchestrator.assets.bronze import (
    bronze_raw_marketdata,
    bronze_raw_orderbook,
    bronze_raw_trades,
)


def test_bronze_raw_trades_asset_key():
    """Bronze raw trades asset has correct key."""
    assert bronze_raw_trades.key == AssetKey("bronze_raw_trades")


def test_bronze_raw_orderbook_asset_key():
    """Bronze raw orderbook asset has correct key."""
    assert bronze_raw_orderbook.key == AssetKey("bronze_raw_orderbook")


def test_bronze_raw_marketdata_asset_key():
    """Bronze raw marketdata asset has correct key."""
    assert bronze_raw_marketdata.key == AssetKey("bronze_raw_marketdata")


def test_bronze_raw_trades_group():
    """Bronze assets belong to the bronze group."""
    assert bronze_raw_trades.group_names_by_key[AssetKey("bronze_raw_trades")] == "bronze"


def test_bronze_raw_orderbook_group():
    """Bronze orderbook belongs to the bronze group."""
    assert bronze_raw_orderbook.group_names_by_key[AssetKey("bronze_raw_orderbook")] == "bronze"


def test_bronze_raw_marketdata_group():
    """Bronze marketdata belongs to the bronze group."""
    assert bronze_raw_marketdata.group_names_by_key[AssetKey("bronze_raw_marketdata")] == "bronze"


def test_bronze_raw_trades_is_partitioned():
    """Bronze raw trades has a daily partition definition."""
    assert bronze_raw_trades.partitions_def is not None


def test_bronze_raw_orderbook_is_partitioned():
    """Bronze raw orderbook has a daily partition definition."""
    assert bronze_raw_orderbook.partitions_def is not None
