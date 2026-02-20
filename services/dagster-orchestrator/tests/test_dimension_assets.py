"""Tests for Dimension asset definitions."""

from dagster import AssetKey

from orchestrator.assets.dimensions import (
    dim_account,
    dim_exchange,
    dim_symbol,
    dim_time,
    dim_trader,
)


def test_dim_symbol_key():
    """dim_symbol has correct asset key."""
    assert dim_symbol.key == AssetKey("dim_symbol")


def test_dim_time_key():
    """dim_time has correct asset key."""
    assert dim_time.key == AssetKey("dim_time")


def test_dim_trader_key():
    """dim_trader has correct asset key."""
    assert dim_trader.key == AssetKey("dim_trader")


def test_dim_exchange_key():
    """dim_exchange has correct asset key."""
    assert dim_exchange.key == AssetKey("dim_exchange")


def test_dim_account_key():
    """dim_account has correct asset key."""
    assert dim_account.key == AssetKey("dim_account")


def test_dim_symbol_group():
    """Dimension assets belong to the dimensions group."""
    assert dim_symbol.group_names_by_key[AssetKey("dim_symbol")] == "dimensions"


def test_dim_time_group():
    """dim_time belongs to dimensions group."""
    assert dim_time.group_names_by_key[AssetKey("dim_time")] == "dimensions"


def test_dim_trader_group():
    """dim_trader belongs to dimensions group."""
    assert dim_trader.group_names_by_key[AssetKey("dim_trader")] == "dimensions"


def test_dim_symbol_not_partitioned():
    """Dimension assets are not partitioned."""
    assert dim_symbol.partitions_def is None


def test_dim_time_not_partitioned():
    """dim_time is not partitioned."""
    assert dim_time.partitions_def is None
