"""masking.py
API-level data masking for the GraphQL service.

Applies field-level masking rules to query results based on the requesting
user's role. This is the application-layer complement to the DuckDB SQL views
in governance/security/masking/masked_views.sql.

Masking strategies:
  hash     → MD5 hex of the original value (pseudonymisation)
  partial  → first N characters + '***'
  redact   → constant '***REDACTED***'
  round    → round numeric to N decimal places

Usage:
    from app.governance.masking import apply_masking

    rows = [{"trade_id": "T1", "account_id": "ACC-123", ...}]
    masked = apply_masking(rows, user_role="data_analyst")
"""

import hashlib
from typing import Any

# ─── Masking rule definitions ─────────────────────────────────────────────────
# Structure:
#   role → field → (strategy, *args)
#
# Supported strategies:
#   ("hash",)           → md5(value)
#   ("partial", n)      → value[:n] + "***"
#   ("redact",)         → "***REDACTED***"
#   ("round", n)        → round(value, n)   (numeric only)

MaskingRules = dict[str, dict[str, tuple]]

MASKING_RULES: MaskingRules = {
    "data_engineer": {},   # No masking — full access
    "admin": {},           # No masking — full access
    "data_scientist": {
        "account_id": ("hash",),
    },
    "data_analyst": {
        "account_id": ("partial", 4),
        "price": ("round", 2),
    },
    "business_user": {
        "account_id": ("redact",),
        "price": ("round", 0),
    },
    "viewer": {
        "account_id": ("redact",),
        "price": ("redact",),
        "quantity": ("redact",),
    },
}

REDACTED = "***REDACTED***"


def _mask_value(value: Any, strategy: tuple) -> Any:
    if value is None:
        return None

    op = strategy[0]
    if op == "redact":
        return REDACTED
    if op == "hash":
        return hashlib.md5(str(value).encode()).hexdigest()
    if op == "partial":
        n = strategy[1]
        s = str(value)
        return s[:n] + "***" if len(s) > n else "***"
    if op == "round":
        n = strategy[1]
        try:
            return round(float(value), n)
        except (TypeError, ValueError):
            return value
    return value


def _effective_rules(user_role: str) -> dict[str, tuple]:
    """Return the masking rules for a given role (falls back to most restrictive)."""
    return MASKING_RULES.get(user_role, MASKING_RULES["viewer"])


def apply_masking(
    rows: list[dict[str, Any]],
    user_role: str,
) -> list[dict[str, Any]]:
    """Apply column-level masking to a list of result rows.

    Args:
        rows: Raw rows from the database query.
        user_role: The requesting user's primary role.

    Returns:
        New list of rows with PII fields masked according to the role's rules.
    """
    rules = _effective_rules(user_role)
    if not rules:
        return rows   # fast path: no masking required

    result = []
    for row in rows:
        masked_row = dict(row)
        for field_name, strategy in rules.items():
            if field_name in masked_row:
                masked_row[field_name] = _mask_value(masked_row[field_name], strategy)
        result.append(masked_row)
    return result


def mask_value(value: Any, user_role: str, field_name: str) -> Any:
    """Mask a single value for the given role and field."""
    rules = _effective_rules(user_role)
    strategy = rules.get(field_name)
    if strategy is None:
        return value
    return _mask_value(value, strategy)
