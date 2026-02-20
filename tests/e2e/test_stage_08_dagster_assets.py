"""Stage 8 — Dagster orchestration asset definitions.

Tests that all pipeline assets exist with correct keys, group names, and
dependency chains.  Uses pytest.importorskip so the suite gracefully skips
when dagster is not installed in the current venv (it requires Python 3.11).
"""

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "services/dagster-orchestrator/src"))

import pytest

dagster = pytest.importorskip("dagster", reason="dagster not installed in this venv")


# ── Bronze ─────────────────────────────────────────────────────────────────────

def test_bronze_trades_asset_defined():
    from orchestrator.assets.bronze.bronze_trades import bronze_raw_trades
    assert isinstance(bronze_raw_trades, dagster.AssetsDefinition)


def test_bronze_orderbook_asset_defined():
    from orchestrator.assets.bronze.bronze_orderbook import bronze_raw_orderbook
    assert isinstance(bronze_raw_orderbook, dagster.AssetsDefinition)


def test_bronze_assets_in_bronze_group():
    from orchestrator.assets.bronze.bronze_trades import bronze_raw_trades
    spec = list(bronze_raw_trades.specs)[0]
    assert spec.group_name == "bronze"


# ── Silver ─────────────────────────────────────────────────────────────────────

def test_silver_trades_asset_defined():
    from orchestrator.assets.silver.silver_trades import silver_trades
    assert isinstance(silver_trades, dagster.AssetsDefinition)


def test_silver_depends_on_bronze_trades():
    from orchestrator.assets.silver.silver_trades import silver_trades
    from dagster import AssetKey
    all_deps = set()
    for dep_set in silver_trades.asset_deps.values():
        all_deps |= dep_set
    assert AssetKey("bronze_raw_trades") in all_deps


def test_silver_assets_in_silver_group():
    from orchestrator.assets.silver.silver_trades import silver_trades
    spec = list(silver_trades.specs)[0]
    assert spec.group_name == "silver"


# ── Gold ───────────────────────────────────────────────────────────────────────

def test_gold_daily_summary_asset_defined():
    from orchestrator.assets.gold.gold_daily_summary import gold_daily_trading_summary
    assert isinstance(gold_daily_trading_summary, dagster.AssetsDefinition)


def test_gold_depends_on_silver():
    from orchestrator.assets.gold.gold_daily_summary import gold_daily_trading_summary
    from dagster import AssetKey
    all_deps = set()
    for dep_set in gold_daily_trading_summary.asset_deps.values():
        all_deps |= dep_set
    assert AssetKey("silver_trades") in all_deps


def test_gold_assets_in_gold_group():
    from orchestrator.assets.gold.gold_daily_summary import gold_daily_trading_summary
    spec = list(gold_daily_trading_summary.specs)[0]
    assert spec.group_name == "gold"


# ── Governance ─────────────────────────────────────────────────────────────────

def test_masked_analyst_asset_defined():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    assert isinstance(masked_silver_trades_analyst, dagster.AssetsDefinition)


def test_pii_retention_asset_defined():
    from orchestrator.assets.governance.pii_retention import pii_retention_enforcement
    assert isinstance(pii_retention_enforcement, dagster.AssetsDefinition)


def test_governance_assets_in_governance_group():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    spec = list(masked_silver_trades_analyst.specs)[0]
    assert spec.group_name == "governance"


# ── Definitions wiring ─────────────────────────────────────────────────────────

def test_all_core_assets_in_definitions():
    from orchestrator.definitions import defs
    from dagster import AssetKey
    keys = {spec.key for spec in defs.get_all_asset_specs()}
    assert AssetKey("bronze_raw_trades") in keys
    assert AssetKey("silver_trades") in keys
    assert AssetKey("gold_daily_trading_summary") in keys


def test_governance_assets_in_definitions():
    from orchestrator.definitions import defs
    from dagster import AssetKey
    keys = {spec.key for spec in defs.get_all_asset_specs()}
    assert AssetKey("masked_silver_trades_analyst") in keys
    assert AssetKey("pii_retention_enforcement") in keys


def test_bronze_silver_gold_dep_chain():
    """Verify the full Bronze → Silver → Gold dependency chain exists."""
    from orchestrator.assets.bronze.bronze_trades import bronze_raw_trades
    from orchestrator.assets.silver.silver_trades import silver_trades
    from orchestrator.assets.gold.gold_daily_summary import gold_daily_trading_summary
    from dagster import AssetKey

    bronze_key = list(bronze_raw_trades.specs)[0].key

    silver_deps = set()
    for dep_set in silver_trades.asset_deps.values():
        silver_deps |= dep_set
    assert bronze_key in silver_deps

    gold_deps = set()
    for dep_set in gold_daily_trading_summary.asset_deps.values():
        gold_deps |= dep_set
    silver_key = list(silver_trades.specs)[0].key
    assert silver_key in gold_deps
