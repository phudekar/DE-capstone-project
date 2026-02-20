"""Tests for app.middleware.cls_middleware — column-level security view selection."""

import pytest
from app.middleware.cls_middleware import cls_view_for_role, primary_role
from app.auth.models import UserContext


# ─── cls_view_for_role ────────────────────────────────────────────────────────


def test_admin_gets_base_table():
    result = cls_view_for_role("admin", "silver_trades")
    assert result == "silver_trades"


def test_data_engineer_gets_base_table():
    result = cls_view_for_role("data_engineer", "silver_trades")
    assert result == "silver_trades"


def test_data_scientist_gets_scientist_view():
    result = cls_view_for_role("data_scientist", "silver_trades")
    assert result == "silver_trades_cls_scientist"


def test_data_analyst_gets_analyst_view():
    result = cls_view_for_role("data_analyst", "silver_trades")
    assert result == "silver_trades_cls_analyst"


def test_analyst_alias_gets_analyst_view():
    result = cls_view_for_role("analyst", "silver_trades")
    assert result == "silver_trades_cls_analyst"


def test_business_user_gets_business_view():
    result = cls_view_for_role("business_user", "silver_trades")
    assert result == "silver_trades_cls_business"


def test_viewer_gets_business_view():
    result = cls_view_for_role("viewer", "silver_trades")
    assert result == "silver_trades_cls_business"


def test_gold_summary_all_roles_get_cls_all():
    for role in ["admin", "data_engineer", "data_scientist", "data_analyst", "analyst",
                 "business_user", "viewer"]:
        result = cls_view_for_role(role, "daily_trading_summary")
        assert result == "daily_summary_cls_all", f"Failed for role: {role}"


def test_unknown_role_uses_most_restrictive_view():
    result = cls_view_for_role("mystery_role", "silver_trades")
    assert result == "silver_trades_cls_business"


def test_unknown_table_returns_base_table():
    result = cls_view_for_role("data_analyst", "nonexistent_table")
    assert result == "nonexistent_table"


# ─── primary_role ─────────────────────────────────────────────────────────────


def test_primary_role_admin_wins():
    user = UserContext(user_id="u", roles=["admin", "viewer", "data_analyst"])
    assert primary_role(user) == "admin"


def test_primary_role_data_engineer():
    user = UserContext(user_id="u", roles=["data_engineer", "viewer"])
    assert primary_role(user) == "data_engineer"


def test_primary_role_data_analyst_over_business():
    user = UserContext(user_id="u", roles=["data_analyst", "business_user"])
    assert primary_role(user) == "data_analyst"


def test_primary_role_viewer_fallback():
    user = UserContext(user_id="u", roles=["viewer"])
    assert primary_role(user) == "viewer"


def test_primary_role_empty_roles():
    user = UserContext(user_id="u", roles=[])
    assert primary_role(user) == "viewer"
