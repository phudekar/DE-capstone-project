"""Tests for app.middleware.rls_middleware — row-level security session vars."""

from unittest.mock import MagicMock, call
from app.auth.models import UserContext
from app.middleware.rls_middleware import apply_rls_session_vars, clear_rls_session_vars


def _make_user(account_id=None, region=None, roles=None):
    user = UserContext(user_id="u1", roles=roles or ["viewer"], account_id=account_id)
    if region is not None:
        user.region = region
    return user


def test_apply_rls_sets_account_id():
    conn = MagicMock()
    user = _make_user(account_id="ACC-001")
    apply_rls_session_vars(conn, user)
    conn.execute.assert_any_call("SET account_id = ?", ["ACC-001"])


def test_apply_rls_sets_region():
    conn = MagicMock()
    user = _make_user(account_id="ACC-001", region="US-EAST")
    apply_rls_session_vars(conn, user)
    conn.execute.assert_any_call("SET user_region = ?", ["US-EAST"])


def test_apply_rls_defaults_empty_account_id():
    conn = MagicMock()
    user = _make_user(account_id=None)
    apply_rls_session_vars(conn, user)
    conn.execute.assert_any_call("SET account_id = ?", [""])


def test_apply_rls_defaults_global_region():
    conn = MagicMock()
    user = _make_user(region=None)
    apply_rls_session_vars(conn, user)
    conn.execute.assert_any_call("SET user_region = ?", ["GLOBAL"])


def test_apply_rls_does_not_raise_on_db_error():
    conn = MagicMock()
    conn.execute.side_effect = Exception("DB unavailable")
    user = _make_user(account_id="ACC-999")
    # Should not raise
    apply_rls_session_vars(conn, user)


def test_clear_rls_session_vars():
    conn = MagicMock()
    clear_rls_session_vars(conn)
    conn.execute.assert_any_call("SET account_id = ''")
    conn.execute.assert_any_call("SET user_region = ''")


def test_clear_rls_does_not_raise_on_db_error():
    conn = MagicMock()
    conn.execute.side_effect = Exception("DB unavailable")
    clear_rls_session_vars(conn)  # Should not raise
