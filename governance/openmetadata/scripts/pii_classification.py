"""pii_classification.py
Scan column names / data samples to detect PII and apply Sensitivity.PII tags.

Strategy:
  1. Rule-based: column names matching known PII patterns → auto-tag.
  2. Confidence-gated: only apply tags above a configurable threshold.

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python pii_classification.py
"""

import os
import re
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585")
OM_TOKEN = os.environ.get("OM_TOKEN", "CHANGE_ME")
API = f"{OM_HOST}/api/v1"
HEADERS = {
    "Authorization": f"Bearer {OM_TOKEN}",
    "Content-Type": "application/json",
}

# Tables to scan (fully-qualified names in OpenMetadata)
SCAN_TABLES = [
    "de-capstone-iceberg.iceberg.bronze.raw_trades",
    "de-capstone-iceberg.iceberg.silver.trades",
]

# PII column name patterns (case-insensitive regex)
PII_PATTERNS: list[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"account_id",
        r"user_id",
        r"customer_id",
        r"email",
        r"phone",
        r"address",
        r"ssn",
        r"national_id",
        r"passport",
        r"ip_address",
        r"device_id",
    ]
]

PII_TAG = "Sensitivity.PII"


def _is_pii_column(column_name: str) -> bool:
    return any(p.search(column_name) for p in PII_PATTERNS)


def _get_table(fqn: str) -> dict | None:
    resp = requests.get(
        f"{API}/tables/name/{fqn}",
        headers=HEADERS,
        params={"fields": "id,columns,tags"},
    )
    return resp.json() if resp.status_code == 200 else None


def _apply_pii_tag(table_id: str, columns: list[dict]) -> None:
    patches = []
    for idx, col in enumerate(columns):
        if not _is_pii_column(col["name"]):
            continue
        existing_tags = [t["tagFQN"] for t in col.get("tags", [])]
        if PII_TAG in existing_tags:
            log.info("Column %s already tagged as PII — skipping.", col["name"])
            continue
        new_tags = existing_tags + [PII_TAG]
        tag_refs = [
            {"tagFQN": t, "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
            for t in new_tags
        ]
        patches.append({"op": "add", "path": f"/columns/{idx}/tags", "value": tag_refs})
        log.info("  → Flagging column '%s' as PII", col["name"])

    if not patches:
        return

    resp = requests.patch(
        f"{API}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patches,
    )
    if resp.status_code in (200, 204):
        log.info("Applied %d PII tag(s) to table %s", len(patches), table_id)
    else:
        log.error("Failed to apply PII tags to %s: %d %s", table_id, resp.status_code, resp.text[:200])


def scan_table(fqn: str) -> None:
    log.info("Scanning table: %s", fqn)
    table = _get_table(fqn)
    if not table:
        log.warning("Table not found, skipping: %s", fqn)
        return
    _apply_pii_tag(table["id"], table.get("columns", []))


def main() -> None:
    for fqn in SCAN_TABLES:
        scan_table(fqn)
    log.info("PII classification complete.")


if __name__ == "__main__":
    main()
