"""cls_middleware.py
Column-Level Security helper for the GraphQL API.

Selects the appropriate CLS view name (or None for full access) based on the
requesting user's primary role. Resolvers use this to direct queries to the
role-scoped DuckDB view instead of the base table.

Usage:
    from app.middleware.cls_middleware import cls_view_for_role

    view = cls_view_for_role(user_role="data_analyst", base_table="silver_trades")
    # Returns: "silver_trades_cls_analyst"
"""

import logging

log = logging.getLogger(__name__)

# Mapping: (base_table, role) → CLS view name
# None means "use the base table directly (full access)"
_CLS_VIEW_MAP: dict[tuple[str, str], str | None] = {
    # silver.trades
    ("silver_trades", "admin"):          None,
    ("silver_trades", "data_engineer"):  None,
    ("silver_trades", "data_scientist"): "silver_trades_cls_scientist",
    ("silver_trades", "data_analyst"):   "silver_trades_cls_analyst",
    ("silver_trades", "analyst"):        "silver_trades_cls_analyst",
    ("silver_trades", "business_user"):  "silver_trades_cls_business",
    ("silver_trades", "viewer"):         "silver_trades_cls_business",
    # gold.daily_trading_summary (no PII — same view for all roles)
    ("daily_trading_summary", "admin"):          "daily_summary_cls_all",
    ("daily_trading_summary", "data_engineer"):  "daily_summary_cls_all",
    ("daily_trading_summary", "data_scientist"): "daily_summary_cls_all",
    ("daily_trading_summary", "data_analyst"):   "daily_summary_cls_all",
    ("daily_trading_summary", "analyst"):        "daily_summary_cls_all",
    ("daily_trading_summary", "business_user"):  "daily_summary_cls_all",
    ("daily_trading_summary", "viewer"):         "daily_summary_cls_all",
}

# Most restrictive fallback for unknown roles
_DEFAULT_VIEW: dict[str, str | None] = {
    "silver_trades":          "silver_trades_cls_business",
    "daily_trading_summary":  "daily_summary_cls_all",
}


def cls_view_for_role(user_role: str, base_table: str) -> str:
    """Return the CLS view name to use for the given role and base table.

    Falls back to the most restrictive view if the role is not mapped.
    Falls back to the base table name if no mapping exists at all.
    """
    view = _CLS_VIEW_MAP.get((base_table, user_role))
    if view is not None:
        return view

    # Explicitly mapped to None = full access
    if (base_table, user_role) in _CLS_VIEW_MAP:
        return base_table

    # Unknown role: use most restrictive default
    default = _DEFAULT_VIEW.get(base_table)
    if default:
        log.debug(
            "Unknown role '%s' for table '%s' — using default CLS view '%s'",
            user_role, base_table, default,
        )
        return default

    log.warning("No CLS mapping for table '%s' — returning base table.", base_table)
    return base_table


def primary_role(user) -> str:
    """Return the user's most privileged role for CLS view selection."""
    role_priority = [
        "admin", "data_engineer", "data_scientist",
        "data_analyst", "analyst", "business_user", "viewer",
    ]
    for role in role_priority:
        if role in getattr(user, "roles", []):
            return role
    return "viewer"
