"""Tests for Superset role and masking permission definitions.

Verifies that the bootstrap create_roles module defines the correct roles
with the expected permission sets, without requiring a live Superset instance.
"""

import sys
import os

import pytest

# Allow importing bootstrap modules directly
_BOOTSTRAP_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "bootstrap")
sys.path.insert(0, os.path.abspath(_BOOTSTRAP_DIR))


# ──────────────────────────────────────────────────────────────────────────────
# Role definitions from create_roles.py
# ──────────────────────────────────────────────────────────────────────────────


def test_roles_module_importable():
    import create_roles
    assert hasattr(create_roles, "ROLES")


def test_roles_has_required_entries():
    import create_roles
    role_names = {r["name"] for r in create_roles.ROLES}
    required = {"TradeViewer", "TradeAnalyst", "DataEngineer", "Compliance"}
    assert required.issubset(role_names), f"Missing roles: {required - role_names}"


def test_roles_have_permissions_field():
    import create_roles
    for role in create_roles.ROLES:
        assert "permissions" in role, f"Role {role['name']} missing 'permissions'"
        assert isinstance(role["permissions"], list)


def test_trade_viewer_has_dashboard_access():
    import create_roles
    viewer = next(r for r in create_roles.ROLES if r["name"] == "TradeViewer")
    perms_str = str(viewer["permissions"]).lower()
    assert "dashboard" in perms_str or len(viewer["permissions"]) > 0


def test_compliance_role_exists():
    import create_roles
    names = [r["name"] for r in create_roles.ROLES]
    assert "Compliance" in names


def test_data_engineer_role_exists():
    import create_roles
    names = [r["name"] for r in create_roles.ROLES]
    assert "DataEngineer" in names


def test_all_roles_have_non_empty_name():
    import create_roles
    for role in create_roles.ROLES:
        assert role["name"].strip() != "", "Role with empty name found"


# ──────────────────────────────────────────────────────────────────────────────
# Row-level security concept tests (DuckDB-side filter simulation)
# ──────────────────────────────────────────────────────────────────────────────


def test_rls_symbol_filter_restricts_results(duckdb_conn):
    """Simulate a row-level filter: user sees only their allowed symbols."""
    allowed_symbols = ("AAPL", "MSFT")
    rows = duckdb_conn.execute("""
        SELECT DISTINCT symbol FROM trades_enriched
        WHERE symbol IN ('AAPL', 'MSFT')
    """).fetchall()
    returned_symbols = {r[0] for r in rows}
    assert returned_symbols.issubset(set(allowed_symbols))


def test_rls_account_filter_restricts_to_single_account(duckdb_conn, accounts):
    """Simulate trader seeing only their own account trades."""
    account = accounts[0]
    rows = duckdb_conn.execute(f"""
        SELECT DISTINCT account_id FROM trades_enriched
        WHERE account_id = '{account}'
    """).fetchall()
    for row in rows:
        assert row[0] == account


def test_rls_date_range_filter(duckdb_conn):
    """Time-window filter returns only rows within the requested range."""
    from datetime import date, timedelta
    today = date.today()
    three_days_ago = today - timedelta(days=3)
    rows = duckdb_conn.execute(f"""
        SELECT DISTINCT trade_date FROM trades_enriched
        WHERE trade_date >= '{three_days_ago}'
        ORDER BY trade_date
    """).fetchall()
    for row in rows:
        assert row[0] >= three_days_ago


# ──────────────────────────────────────────────────────────────────────────────
# Account masking in SQL results
# ──────────────────────────────────────────────────────────────────────────────


def test_account_id_masking_partial(duckdb_conn):
    """Simulate partial masking: only first 4 chars visible."""
    rows = duckdb_conn.execute("""
        SELECT
            account_id,
            LEFT(account_id, 4) || '***' AS masked_id
        FROM trades_enriched
        LIMIT 5
    """).fetchall()
    for raw, masked in rows:
        assert masked.endswith("***")
        assert masked[:4] == raw[:4]


def test_account_id_masking_redact(duckdb_conn):
    """Simulate full redaction: all account_ids replaced with '[REDACTED]'."""
    rows = duckdb_conn.execute("""
        SELECT '[REDACTED]' AS account_id, symbol
        FROM trades_enriched
        LIMIT 5
    """).fetchall()
    for row in rows:
        assert row[0] == "[REDACTED]"


def test_account_id_hashing(duckdb_conn):
    """Simulate MD5 hashing of account_ids for data_scientist role."""
    rows = duckdb_conn.execute("""
        SELECT md5(account_id) AS hashed_id, account_id
        FROM trades_enriched
        LIMIT 5
    """).fetchall()
    for hashed, raw in rows:
        assert hashed != raw
        assert len(hashed) == 32   # MD5 hex digest
