"""Tests for app.governance.masking — data masking module."""

import pytest
from app.governance.masking import (
    MASKING_RULES,
    REDACTED,
    apply_masking,
    mask_value,
)


# ─── MASKING_RULES structure ──────────────────────────────────────────────────


def test_masking_rules_has_required_roles():
    required = {"admin", "data_engineer", "data_scientist", "data_analyst", "business_user"}
    assert required.issubset(MASKING_RULES.keys())


def test_admin_has_no_masking_rules():
    assert MASKING_RULES["admin"] == {}


def test_data_engineer_has_no_masking_rules():
    assert MASKING_RULES["data_engineer"] == {}


def test_data_scientist_masks_account_id_as_hash():
    rules = MASKING_RULES["data_scientist"]
    assert "account_id" in rules
    assert rules["account_id"][0] == "hash"


def test_data_analyst_masks_account_id_as_partial():
    rules = MASKING_RULES["data_analyst"]
    assert "account_id" in rules
    assert rules["account_id"][0] == "partial"


def test_business_user_redacts_account_id():
    rules = MASKING_RULES["business_user"]
    assert "account_id" in rules
    assert rules["business_user" and "account_id"][0] == "redact"


# ─── mask_value ───────────────────────────────────────────────────────────────


def test_mask_value_none_returns_none():
    assert mask_value(None, "data_analyst", "account_id") is None


def test_mask_value_no_rule_returns_original():
    assert mask_value("abc", "data_analyst", "symbol") == "abc"


def test_mask_value_hash_returns_hex_string():
    result = mask_value("ACC-123", "data_scientist", "account_id")
    assert isinstance(result, str)
    assert len(result) == 32   # MD5 hex
    assert result != "ACC-123"


def test_mask_value_hash_is_deterministic():
    v1 = mask_value("ACC-123", "data_scientist", "account_id")
    v2 = mask_value("ACC-123", "data_scientist", "account_id")
    assert v1 == v2


def test_mask_value_partial_keeps_prefix():
    result = mask_value("ACC-12345", "data_analyst", "account_id")
    assert result.startswith("ACC-")
    assert "***" in result


def test_mask_value_partial_short_string_returns_stars():
    # String shorter than n → "***"
    result = mask_value("AB", "data_analyst", "account_id")
    assert result == "***"


def test_mask_value_redact_returns_constant():
    result = mask_value("ACC-123", "business_user", "account_id")
    assert result == REDACTED


def test_mask_value_round_numeric():
    result = mask_value(123.456789, "data_analyst", "price")
    assert result == pytest.approx(123.46)


def test_mask_value_round_to_zero_dp():
    result = mask_value(123.456789, "business_user", "price")
    assert result == pytest.approx(123.0)


# ─── apply_masking ────────────────────────────────────────────────────────────


def test_apply_masking_admin_returns_rows_unchanged():
    rows = [{"account_id": "ACC-1", "symbol": "AAPL", "price": 150.00}]
    result = apply_masking(rows, user_role="admin")
    assert result == rows


def test_apply_masking_data_engineer_returns_rows_unchanged():
    rows = [{"account_id": "ACC-1", "symbol": "AAPL"}]
    result = apply_masking(rows, user_role="data_engineer")
    assert result == rows


def test_apply_masking_scientist_hashes_account_id():
    rows = [{"account_id": "ACC-1", "symbol": "AAPL"}]
    result = apply_masking(rows, user_role="data_scientist")
    assert result[0]["account_id"] != "ACC-1"
    assert len(result[0]["account_id"]) == 32   # MD5 hex
    assert result[0]["symbol"] == "AAPL"        # untouched


def test_apply_masking_business_redacts_account_id():
    rows = [{"account_id": "ACC-1", "symbol": "AAPL", "price": 150.55}]
    result = apply_masking(rows, user_role="business_user")
    assert result[0]["account_id"] == REDACTED


def test_apply_masking_missing_field_no_error():
    rows = [{"symbol": "AAPL"}]   # no account_id
    result = apply_masking(rows, user_role="data_analyst")
    assert result[0] == {"symbol": "AAPL"}


def test_apply_masking_empty_rows():
    assert apply_masking([], user_role="data_analyst") == []


def test_apply_masking_unknown_role_uses_viewer_rules():
    rows = [{"account_id": "ACC-1", "price": 100.0}]
    result = apply_masking(rows, user_role="unknown_role")
    assert result[0]["account_id"] == REDACTED


def test_apply_masking_preserves_row_count():
    rows = [{"account_id": f"ACC-{i}", "symbol": "X"} for i in range(10)]
    result = apply_masking(rows, user_role="data_analyst")
    assert len(result) == 10


def test_apply_masking_does_not_mutate_original():
    original = [{"account_id": "ACC-1"}]
    apply_masking(original, user_role="business_user")
    assert original[0]["account_id"] == "ACC-1"
