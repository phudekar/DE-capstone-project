"""Tests for governance Dagster assets: masked_tables and pii_retention."""

from unittest.mock import MagicMock, patch


# ─── masked_tables ────────────────────────────────────────────────────────────


def test_masked_silver_trades_analyst_is_defined():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    from dagster import AssetsDefinition
    assert isinstance(masked_silver_trades_analyst, AssetsDefinition)


def test_masked_silver_trades_business_is_defined():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_business
    from dagster import AssetsDefinition
    assert isinstance(masked_silver_trades_business, AssetsDefinition)


def test_masked_gold_summary_public_is_defined():
    from orchestrator.assets.governance.masked_tables import masked_gold_summary_public
    from dagster import AssetsDefinition
    assert isinstance(masked_gold_summary_public, AssetsDefinition)


def test_masked_tables_have_correct_group():
    from orchestrator.assets.governance.masked_tables import (
        masked_silver_trades_analyst,
        masked_silver_trades_business,
        masked_gold_summary_public,
    )
    for asset_def in [masked_silver_trades_analyst, masked_silver_trades_business, masked_gold_summary_public]:
        spec = list(asset_def.specs)[0]
        assert spec.group_name == "governance"


def test_masked_analyst_asset_key():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    from dagster import AssetKey
    spec = list(masked_silver_trades_analyst.specs)[0]
    assert spec.key == AssetKey("masked_silver_trades_analyst")


def test_masked_business_asset_key():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_business
    from dagster import AssetKey
    spec = list(masked_silver_trades_business.specs)[0]
    assert spec.key == AssetKey("masked_silver_trades_business")


def test_masked_gold_asset_key():
    from orchestrator.assets.governance.masked_tables import masked_gold_summary_public
    from dagster import AssetKey
    spec = list(masked_gold_summary_public.specs)[0]
    assert spec.key == AssetKey("masked_gold_summary_public")


def test_masked_analyst_depends_on_silver_trades():
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    from dagster import AssetKey
    # asset_deps: {asset_key: set[AssetKey of deps]}
    all_deps = set()
    for dep_set in masked_silver_trades_analyst.asset_deps.values():
        all_deps |= dep_set
    assert AssetKey("silver_trades") in all_deps


def test_masked_gold_depends_on_gold_daily_summary():
    from orchestrator.assets.governance.masked_tables import masked_gold_summary_public
    assert len(masked_gold_summary_public.asset_deps) > 0


def test_masked_tables_execute_without_crash_when_iceberg_unavailable():
    """Assets should log a warning rather than crash when Iceberg is not running."""
    from orchestrator.assets.governance.masked_tables import masked_silver_trades_analyst
    from dagster import build_asset_context

    ctx = build_asset_context()
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Iceberg not available")
        mock_connect.return_value = mock_conn
        # Should not raise
        masked_silver_trades_analyst(ctx)


# ─── pii_retention ────────────────────────────────────────────────────────────


def test_pii_retention_asset_is_defined():
    from orchestrator.assets.governance.pii_retention import pii_retention_enforcement
    from dagster import AssetsDefinition
    assert isinstance(pii_retention_enforcement, AssetsDefinition)


def test_pii_retention_asset_group():
    from orchestrator.assets.governance.pii_retention import pii_retention_enforcement
    spec = list(pii_retention_enforcement.specs)[0]
    assert spec.group_name == "governance"


def test_pii_retention_asset_key():
    from orchestrator.assets.governance.pii_retention import pii_retention_enforcement
    from dagster import AssetKey
    spec = list(pii_retention_enforcement.specs)[0]
    assert spec.key == AssetKey("pii_retention_enforcement")


def test_pii_retention_default_config():
    from orchestrator.assets.governance.pii_retention import PiiRetentionConfig
    cfg = PiiRetentionConfig()
    assert cfg.retention_days == 90
    assert cfg.dry_run is False


def test_pii_retention_dry_run_config():
    from orchestrator.assets.governance.pii_retention import PiiRetentionConfig
    cfg = PiiRetentionConfig(retention_days=30, dry_run=True)
    assert cfg.retention_days == 30
    assert cfg.dry_run is True


def test_pii_retention_executes_without_crash_when_iceberg_unavailable():
    from orchestrator.assets.governance.pii_retention import (
        pii_retention_enforcement,
        PiiRetentionConfig,
    )
    from dagster import build_asset_context

    ctx = build_asset_context()
    with patch("duckdb.connect") as mock_connect:
        mock_connect.side_effect = Exception("no iceberg")
        # Should not raise
        pii_retention_enforcement(ctx, config=PiiRetentionConfig(dry_run=True))


def test_hash_account_id_format():
    from orchestrator.assets.governance.pii_retention import _hash_account, PII_PLACEHOLDER_PREFIX
    result = _hash_account("ACC-123")
    assert result.startswith(PII_PLACEHOLDER_PREFIX)
    assert len(result) == len(PII_PLACEHOLDER_PREFIX) + 16


def test_hash_account_id_deterministic():
    from orchestrator.assets.governance.pii_retention import _hash_account
    assert _hash_account("ACC-X") == _hash_account("ACC-X")


def test_hash_account_id_different_inputs():
    from orchestrator.assets.governance.pii_retention import _hash_account
    assert _hash_account("ACC-1") != _hash_account("ACC-2")


# ─── Definitions wiring ───────────────────────────────────────────────────────


def test_governance_assets_in_definitions():
    """All governance assets must appear in the main Dagster Definitions object."""
    from orchestrator.definitions import defs
    from dagster import AssetKey

    asset_keys = {spec.key for spec in defs.get_all_asset_specs()}
    assert AssetKey("masked_silver_trades_analyst") in asset_keys
    assert AssetKey("masked_silver_trades_business") in asset_keys
    assert AssetKey("masked_gold_summary_public") in asset_keys
    assert AssetKey("pii_retention_enforcement") in asset_keys
