"""pii_access_logger.py
Structured audit logger for PII field access events.

Each access event is written as a JSON line to a rotating audit log file.
Events can also be forwarded to stdout for ingestion by a log aggregator
(e.g., Fluentd, Logstash, CloudWatch).

Schema (per event):
  {
    "event_type":  "PII_ACCESS",
    "timestamp":   "2024-01-15T09:30:00.000Z",
    "user_id":     "user-abc",
    "user_role":   "data_analyst",
    "resource":    "silver.trades",
    "columns":     ["account_id"],
    "operation":   "SELECT",
    "query_hash":  "sha256hex",
    "ip_address":  "10.0.0.1",
    "granted":     true
  }

Usage:
    from governance.audit.pii_access_logger import PiiAccessLogger

    logger = PiiAccessLogger()
    logger.log_access(
        user_id="u1",
        user_role="data_analyst",
        resource="silver.trades",
        columns=["account_id"],
        operation="SELECT",
        query="SELECT account_id FROM ...",
        ip_address="10.0.0.1",
        granted=True,
    )
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ─── Configuration ────────────────────────────────────────────────────────────

AUDIT_LOG_DIR = Path(os.environ.get("AUDIT_LOG_DIR", "/var/log/de-capstone/audit"))
AUDIT_LOG_FILE = AUDIT_LOG_DIR / "pii_access.jsonl"
MAX_BYTES = 50 * 1024 * 1024   # 50 MB per file
BACKUP_COUNT = 10

# Columns classified as PII — used to auto-detect when logging is required
PII_COLUMNS: frozenset[str] = frozenset({
    "account_id",
    "user_id",
    "customer_id",
    "email",
    "phone",
    "ip_address",
    "device_id",
    "ssn",
    "national_id",
})


def _setup_audit_handler() -> logging.Logger:
    AUDIT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    audit_logger = logging.getLogger("pii_audit")
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False

    if not audit_logger.handlers:
        handler = RotatingFileHandler(
            AUDIT_LOG_FILE,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        audit_logger.addHandler(handler)

        # Also log to stdout for container log aggregation
        stdout_handler = logging.StreamHandler()
        stdout_handler.setFormatter(logging.Formatter("%(message)s"))
        audit_logger.addHandler(stdout_handler)

    return audit_logger


_audit_logger = _setup_audit_handler()


class PiiAccessLogger:
    """Thread-safe PII access event logger."""

    def log_access(
        self,
        *,
        user_id: str,
        user_role: str,
        resource: str,
        columns: list[str],
        operation: str,
        query: str = "",
        ip_address: str = "unknown",
        granted: bool = True,
    ) -> None:
        """Log a single PII access event."""
        pii_cols = [c for c in columns if c in PII_COLUMNS]
        if not pii_cols and granted:
            # Only log if PII columns are actually involved
            return

        event = {
            "event_type": "PII_ACCESS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "user_role": user_role,
            "resource": resource,
            "columns": pii_cols or columns,
            "operation": operation,
            "query_hash": hashlib.sha256(query.encode()).hexdigest() if query else "",
            "ip_address": ip_address,
            "granted": granted,
        }
        _audit_logger.info(json.dumps(event, ensure_ascii=False))

    def log_denial(
        self,
        *,
        user_id: str,
        user_role: str,
        resource: str,
        reason: str,
        ip_address: str = "unknown",
    ) -> None:
        """Log an access denial event."""
        event = {
            "event_type": "ACCESS_DENIED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "user_role": user_role,
            "resource": resource,
            "reason": reason,
            "ip_address": ip_address,
            "granted": False,
        }
        _audit_logger.info(json.dumps(event, ensure_ascii=False))


# Module-level singleton for convenience
pii_access_logger = PiiAccessLogger()
