"""right_to_deletion.py
GDPR / CCPA Right-to-Deletion workflow for the DE Capstone lakehouse.

Supports two strategies:
  1. Pseudonymise  – replace PII with a deterministic hash (preserves row count
                     and referential integrity for analytics).
  2. Hard delete   – remove rows matching the account_id entirely.

Strategy is chosen per-table via the DELETION_STRATEGY constant.

Usage (CLI):
    python right_to_deletion.py --account-id ACC-12345 --dry-run
    python right_to_deletion.py --account-id ACC-12345 --execute

Usage (programmatic):
    from governance.audit.right_to_deletion import RightToDeletion
    result = RightToDeletion().process(account_id="ACC-12345", dry_run=False)
"""

import argparse
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
DELETION_REPORT_DIR = os.environ.get("DELETION_REPORT_DIR", "/var/log/de-capstone/deletion")

Strategy = Literal["pseudonymise", "hard_delete"]

@dataclass
class TableConfig:
    namespace: str
    table: str
    account_id_column: str
    strategy: Strategy


TABLES: list[TableConfig] = [
    TableConfig("bronze", "raw_trades",           "account_id", "pseudonymise"),
    TableConfig("silver", "trades",               "account_id", "pseudonymise"),
    # Gold tables are aggregates — account_id is not present
]

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _hash_account(account_id: str) -> str:
    return "DELETED-" + hashlib.sha256(account_id.encode()).hexdigest()[:16]


@dataclass
class DeletionResult:
    account_id: str
    dry_run: bool
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tables_processed: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "account_id": self.account_id,
            "dry_run": self.dry_run,
            "timestamp": self.timestamp,
            "tables_processed": self.tables_processed,
            "errors": self.errors,
        }


class RightToDeletion:
    """Execute GDPR right-to-deletion for a given account_id."""

    def __init__(self) -> None:
        self.conn = duckdb.connect()
        self._install_iceberg()

    def _install_iceberg(self) -> None:
        self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        self.conn.execute(
            f"""
            ATTACH 'rest' AS iceberg_catalog (
                TYPE ICEBERG,
                ENDPOINT '{ICEBERG_CATALOG_URI}'
            )
            """
        )

    def _count_rows(self, cfg: TableConfig, account_id: str) -> int:
        try:
            result = self.conn.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM iceberg_catalog.{cfg.namespace}.{cfg.table}
                WHERE {cfg.account_id_column} = ?
                """,
                [account_id],
            ).fetchone()
            return result[0] if result else 0
        except Exception as exc:
            log.warning("Could not count rows in %s.%s: %s", cfg.namespace, cfg.table, exc)
            return -1

    def _pseudonymise(self, cfg: TableConfig, account_id: str) -> int:
        placeholder = _hash_account(account_id)
        try:
            self.conn.execute(
                f"""
                UPDATE iceberg_catalog.{cfg.namespace}.{cfg.table}
                SET {cfg.account_id_column} = ?
                WHERE {cfg.account_id_column} = ?
                """,
                [placeholder, account_id],
            )
            return self.conn.execute("SELECT changes()").fetchone()[0]
        except Exception as exc:
            log.error("Pseudonymisation failed for %s.%s: %s", cfg.namespace, cfg.table, exc)
            return -1

    def _hard_delete(self, cfg: TableConfig, account_id: str) -> int:
        try:
            self.conn.execute(
                f"""
                DELETE FROM iceberg_catalog.{cfg.namespace}.{cfg.table}
                WHERE {cfg.account_id_column} = ?
                """,
                [account_id],
            )
            return self.conn.execute("SELECT changes()").fetchone()[0]
        except Exception as exc:
            log.error("Hard delete failed for %s.%s: %s", cfg.namespace, cfg.table, exc)
            return -1

    def process(self, account_id: str, dry_run: bool = True) -> DeletionResult:
        result = DeletionResult(account_id=account_id, dry_run=dry_run)

        for cfg in TABLES:
            affected_rows = self._count_rows(cfg, account_id)
            entry: dict = {
                "namespace": cfg.namespace,
                "table": cfg.table,
                "strategy": cfg.strategy,
                "affected_rows": affected_rows,
                "executed": False,
            }

            if affected_rows == 0:
                log.info("No rows for account %s in %s.%s", account_id, cfg.namespace, cfg.table)
                result.tables_processed.append(entry)
                continue

            log.info(
                "%s: %d rows in %s.%s (strategy=%s)",
                "DRY-RUN" if dry_run else "EXECUTE",
                affected_rows,
                cfg.namespace,
                cfg.table,
                cfg.strategy,
            )

            if not dry_run:
                if cfg.strategy == "pseudonymise":
                    rows = self._pseudonymise(cfg, account_id)
                else:
                    rows = self._hard_delete(cfg, account_id)
                entry["executed"] = True
                entry["rows_modified"] = rows

            result.tables_processed.append(entry)

        self._save_report(result)
        return result

    def _save_report(self, result: DeletionResult) -> None:
        import pathlib
        report_dir = pathlib.Path(DELETION_REPORT_DIR)
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        report_file = report_dir / f"deletion_{result.account_id}_{ts}.json"
        report_file.write_text(json.dumps(result.to_dict(), indent=2))
        log.info("Deletion report saved: %s", report_file)


def main() -> None:
    parser = argparse.ArgumentParser(description="GDPR Right-to-Deletion workflow.")
    parser.add_argument("--account-id", required=True, help="Account ID to delete/pseudonymise.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="Show what would be done.")
    group.add_argument("--execute", action="store_true", help="Actually perform the deletion.")
    args = parser.parse_args()

    worker = RightToDeletion()
    result = worker.process(account_id=args.account_id, dry_run=args.dry_run)

    print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    main()
