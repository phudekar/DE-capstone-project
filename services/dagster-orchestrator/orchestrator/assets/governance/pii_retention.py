"""pii_retention.py
Dagster asset for PII retention enforcement.

Runs on a scheduled basis to pseudonymise or delete PII in records older than
the configured retention window. This implements the data minimisation
principle required by GDPR Article 5(1)(e).

Retention policy (configurable via config):
  bronze.raw_trades  → pseudonymise account_id after 90 days
  silver.trades      → pseudonymise account_id after 90 days
  Records in gold layer contain no PII → no action required.

The asset is idempotent: running it multiple times on the same day is safe
because already-pseudonymised rows (starting with "DELETED-") are skipped.
"""

import hashlib
import logging
from datetime import date, timedelta

from dagster import AssetExecutionContext, AssetKey, Config, asset

logger = logging.getLogger(__name__)

# ─── Retention configuration ──────────────────────────────────────────────────

DEFAULT_RETENTION_DAYS = 90
PII_PLACEHOLDER_PREFIX = "DELETED-"


class PiiRetentionConfig(Config):
    retention_days: int = DEFAULT_RETENTION_DAYS
    dry_run: bool = False


def _hash_account(account_id: str) -> str:
    return PII_PLACEHOLDER_PREFIX + hashlib.sha256(account_id.encode()).hexdigest()[:16]


def _enforce_retention_for_table(
    conn,
    namespace: str,
    table: str,
    cutoff_date: date,
    dry_run: bool,
) -> int:
    """Pseudonymise account_id for rows older than cutoff_date.

    Returns the number of rows affected (or would be affected in dry-run mode).
    """
    try:
        # Count rows that need pseudonymisation
        count_result = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM iceberg_catalog.{namespace}.{table}
            WHERE trade_date < ?
              AND account_id NOT LIKE '{PII_PLACEHOLDER_PREFIX}%'
              AND account_id IS NOT NULL
            """,
            [cutoff_date.isoformat()],
        ).fetchone()
        count = count_result[0] if count_result else 0

        if count == 0:
            logger.info("No rows to pseudonymise in %s.%s", namespace, table)
            return 0

        logger.info(
            "%s: %d rows in %s.%s older than %s require pseudonymisation.",
            "DRY-RUN" if dry_run else "EXECUTE",
            count,
            namespace,
            table,
            cutoff_date,
        )

        if not dry_run:
            # DuckDB scalar UDF for deterministic pseudonymisation
            conn.create_function(
                "hash_account_id",
                lambda v: _hash_account(v) if v else v,
                ["VARCHAR"],
                "VARCHAR",
            )
            conn.execute(
                f"""
                UPDATE iceberg_catalog.{namespace}.{table}
                SET account_id = hash_account_id(account_id)
                WHERE trade_date < ?
                  AND account_id NOT LIKE '{PII_PLACEHOLDER_PREFIX}%'
                  AND account_id IS NOT NULL
                """,
                [cutoff_date.isoformat()],
            )
            logger.info("Pseudonymised %d rows in %s.%s", count, namespace, table)

        return count
    except Exception as exc:
        logger.error("Retention enforcement failed for %s.%s: %s", namespace, table, exc)
        return 0


@asset(
    key=AssetKey("pii_retention_enforcement"),
    group_name="governance",
    description=(
        "Pseudonymises PII (account_id) in bronze and silver tables older than "
        "the configured retention window (default: 90 days). "
        "Implements GDPR Article 5(1)(e) data minimisation."
    ),
    kinds={"iceberg", "duckdb"},
)
def pii_retention_enforcement(
    context: AssetExecutionContext,
    config: PiiRetentionConfig,
) -> None:
    """Enforce PII retention by pseudonymising old account_ids."""
    cutoff = date.today() - timedelta(days=config.retention_days)
    context.log.info(
        "PII retention enforcement: cutoff=%s, dry_run=%s",
        cutoff,
        config.dry_run,
    )

    tables_to_process = [
        ("bronze", "raw_trades"),
        ("silver", "trades"),
    ]

    total_affected = 0
    try:
        import duckdb
        from orchestrator.config import settings as orch_settings

        catalog_uri = getattr(orch_settings, "iceberg_catalog_uri", "http://iceberg-rest:8181")
        conn = duckdb.connect()
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        conn.execute(
            f"""
            ATTACH 'rest' AS iceberg_catalog (
                TYPE ICEBERG,
                ENDPOINT '{catalog_uri}'
            )
            """
        )

        for namespace, table in tables_to_process:
            affected = _enforce_retention_for_table(
                conn, namespace, table, cutoff, config.dry_run
            )
            total_affected += affected

    except Exception as exc:
        context.log.warning(
            "Could not connect to Iceberg catalog for PII retention (may not be running): %s",
            exc,
        )

    context.log.info(
        "PII retention complete: %d rows %s.",
        total_affected,
        "would be pseudonymised" if config.dry_run else "pseudonymised",
    )
    context.add_output_metadata({
        "cutoff_date": cutoff.isoformat(),
        "retention_days": config.retention_days,
        "dry_run": config.dry_run,
        "rows_affected": total_affected,
    })
