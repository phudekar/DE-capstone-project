"""Tests for governance.audit.pii_access_logger."""

import json
import sys
import os
import tempfile
import pytest

# Add governance to path so we can import the module directly
_GOVERNANCE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..", "governance"
)
sys.path.insert(0, os.path.abspath(_GOVERNANCE_DIR))

# Set AUDIT_LOG_DIR before any import of pii_access_logger to avoid
# /var/log permission errors at module load time.
_TMP_AUDIT_DIR = tempfile.mkdtemp(prefix="pii_audit_test_")
os.environ.setdefault("AUDIT_LOG_DIR", _TMP_AUDIT_DIR)


def test_pii_access_logger_instantiates():
    import importlib
    import audit.pii_access_logger as mod
    importlib.reload(mod)
    logger = mod.PiiAccessLogger()
    assert logger is not None


def test_pii_columns_set_non_empty():
    import importlib
    import audit.pii_access_logger as mod
    importlib.reload(mod)
    assert len(mod.PII_COLUMNS) > 0
    assert "account_id" in mod.PII_COLUMNS


def test_log_access_does_not_raise(tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_LOG_DIR", str(tmp_path))
    # Re-import to pick up new env var
    import importlib
    import audit.pii_access_logger as mod
    importlib.reload(mod)

    logger = mod.PiiAccessLogger()
    logger.log_access(
        user_id="u1",
        user_role="data_analyst",
        resource="silver.trades",
        columns=["account_id"],
        operation="SELECT",
        query="SELECT account_id FROM silver.trades",
        ip_address="10.0.0.1",
        granted=True,
    )


def test_log_denial_does_not_raise(tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_LOG_DIR", str(tmp_path))
    import importlib
    import audit.pii_access_logger as mod
    importlib.reload(mod)

    logger = mod.PiiAccessLogger()
    logger.log_denial(
        user_id="u1",
        user_role="viewer",
        resource="bronze.raw_trades",
        reason="Insufficient role",
        ip_address="10.0.0.2",
    )


def test_log_access_skips_non_pii_columns(tmp_path, monkeypatch, caplog):
    """No log entry when columns contain no PII fields."""
    monkeypatch.setenv("AUDIT_LOG_DIR", str(tmp_path))
    import importlib
    import audit.pii_access_logger as mod
    importlib.reload(mod)

    logger = mod.PiiAccessLogger()
    import logging
    with caplog.at_level(logging.INFO, logger="pii_audit"):
        logger.log_access(
            user_id="u1",
            user_role="data_analyst",
            resource="silver.trades",
            columns=["symbol", "price"],   # no PII columns
            operation="SELECT",
            granted=True,
        )
    # The PII audit log should be silent when no PII columns accessed
    assert "PII_ACCESS" not in caplog.text


def test_module_level_singleton_exists():
    from audit.pii_access_logger import pii_access_logger
    assert pii_access_logger is not None
