"""apply_tags.py
Apply DataLayer and DataDomain classification tags to tables in OpenMetadata.

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python apply_tags.py
"""

import os
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

# fqn -> list of tag FQNs
TABLE_TAGS: dict[str, list[str]] = {
    "de-capstone-iceberg.iceberg.bronze.raw_trades": [
        "DataLayer.Bronze", "DataDomain.Trade", "Sensitivity.Confidential"
    ],
    "de-capstone-iceberg.iceberg.bronze.raw_order_book": [
        "DataLayer.Bronze", "DataDomain.Order", "Sensitivity.Confidential"
    ],
    "de-capstone-iceberg.iceberg.silver.trades": [
        "DataLayer.Silver", "DataDomain.Trade", "Sensitivity.Restricted"
    ],
    "de-capstone-iceberg.iceberg.silver.ohlcv": [
        "DataLayer.Silver", "DataDomain.Analytics", "Sensitivity.Restricted"
    ],
    "de-capstone-iceberg.iceberg.gold.daily_trading_summary": [
        "DataLayer.Gold", "DataDomain.Analytics", "Sensitivity.Public"
    ],
    "de-capstone-iceberg.iceberg.gold.top_movers": [
        "DataLayer.Gold", "DataDomain.Analytics", "Sensitivity.Public"
    ],
}

# fqn -> column -> list of tag FQNs
COLUMN_TAGS: dict[str, dict[str, list[str]]] = {
    "de-capstone-iceberg.iceberg.bronze.raw_trades": {
        "account_id": ["Sensitivity.PII"],
    },
    "de-capstone-iceberg.iceberg.silver.trades": {
        "account_id": ["Sensitivity.PII"],
    },
}


def _get_table(fqn: str) -> dict | None:
    resp = requests.get(
        f"{API}/tables/name/{fqn}",
        headers=HEADERS,
        params={"fields": "id,columns,tags"},
    )
    return resp.json() if resp.status_code == 200 else None


def _tag_refs(tag_fqns: list[str]) -> list[dict]:
    return [{"tagFQN": t, "source": "Classification", "labelType": "Manual", "state": "Confirmed"}
            for t in tag_fqns]


def apply_table_tags(fqn: str, tags: list[str]) -> None:
    table = _get_table(fqn)
    if not table:
        log.warning("Table not found: %s", fqn)
        return
    table_id = table["id"]
    existing = [t["tagFQN"] for t in table.get("tags", [])]
    new_tags = existing + [t for t in tags if t not in existing]
    patch = [{"op": "add", "path": "/tags", "value": _tag_refs(new_tags)}]
    resp = requests.patch(
        f"{API}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch,
    )
    if resp.status_code in (200, 204):
        log.info("Tagged table %s with %s", fqn, tags)
    else:
        log.error("Failed to tag %s: %d %s", fqn, resp.status_code, resp.text[:200])


def apply_column_tags(fqn: str, column: str, tags: list[str]) -> None:
    table = _get_table(fqn)
    if not table:
        return
    table_id = table["id"]
    columns = table.get("columns", [])
    idx = next((i for i, c in enumerate(columns) if c["name"] == column), None)
    if idx is None:
        log.warning("Column %s not found in %s", column, fqn)
        return
    existing = [t["tagFQN"] for t in columns[idx].get("tags", [])]
    new_tags = existing + [t for t in tags if t not in existing]
    patch = [{"op": "add", "path": f"/columns/{idx}/tags", "value": _tag_refs(new_tags)}]
    resp = requests.patch(
        f"{API}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch,
    )
    if resp.status_code in (200, 204):
        log.info("Tagged column %s.%s with %s", fqn, column, tags)
    else:
        log.error("Failed column tag %s.%s: %d", fqn, column, resp.status_code)


def main() -> None:
    log.info("Applying table-level tags (%d tables)...", len(TABLE_TAGS))
    for fqn, tags in TABLE_TAGS.items():
        apply_table_tags(fqn, tags)

    log.info("Applying column-level PII tags...")
    for fqn, col_map in COLUMN_TAGS.items():
        for col, tags in col_map.items():
            apply_column_tags(fqn, col, tags)

    log.info("Done.")


if __name__ == "__main__":
    main()
