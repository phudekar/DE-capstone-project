"""rls_middleware.py
Row-Level Security helper for the GraphQL API.

Sets the DuckDB session variable `account_id` and `user_region` before each
query so that the RLS views in governance/security/rls/ filter correctly.

Not a Starlette middleware — this is a context helper called within the
GraphQL resolver layer where the DuckDB connection is available.

Usage:
    from app.middleware.rls_middleware import apply_rls_session_vars

    # Inside a resolver or context setup:
    apply_rls_session_vars(conn, user)
"""

import logging

log = logging.getLogger(__name__)

# Default region for users without an assigned region
DEFAULT_REGION = "GLOBAL"


def apply_rls_session_vars(conn, user) -> None:
    """Set DuckDB session variables for row-level security.

    Args:
        conn: A DuckDB connection (duckdb.DuckDBPyConnection).
        user: A UserContext with account_id and optional region attribute.
    """
    account_id = getattr(user, "account_id", None) or ""
    region = getattr(user, "region", None) or DEFAULT_REGION

    try:
        conn.execute("SET account_id = ?", [account_id])
        conn.execute("SET user_region = ?", [region])
    except Exception as exc:
        # Non-fatal: RLS views will return empty if session vars are unset,
        # which is the safe default.
        log.warning("Failed to set RLS session variables: %s", exc)


def clear_rls_session_vars(conn) -> None:
    """Clear RLS session variables (reset to empty strings)."""
    try:
        conn.execute("SET account_id = ''")
        conn.execute("SET user_region = ''")
    except Exception as exc:
        log.debug("Failed to clear RLS session variables: %s", exc)
