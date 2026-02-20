"""schema_change_detector.py
Detect breaking schema changes by comparing the current OpenMetadata column
schema against a persisted baseline snapshot.

A "breaking" change is:
  - Column removed
  - Column type changed
  - Column renamed (detected as remove + add)

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python schema_change_detector.py \
        --snapshot-dir /tmp/schema-snapshots
"""

import os
import json
import logging
import argparse
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585")
OM_TOKEN = os.environ.get("OM_TOKEN", "CHANGE_ME")
API = f"{OM_HOST}/api/v1"
HEADERS = {"Authorization": f"Bearer {OM_TOKEN}"}

WATCH_TABLES = [
    "de-capstone-iceberg.iceberg.bronze.raw_trades",
    "de-capstone-iceberg.iceberg.silver.trades",
    "de-capstone-iceberg.iceberg.silver.ohlcv",
    "de-capstone-iceberg.iceberg.gold.daily_trading_summary",
]


def _snapshot_path(snapshot_dir: Path, fqn: str) -> Path:
    safe_name = fqn.replace("/", "_").replace(".", "_")
    return snapshot_dir / f"{safe_name}.json"


def _fetch_columns(fqn: str) -> list[dict] | None:
    resp = requests.get(
        f"{API}/tables/name/{fqn}",
        headers=HEADERS,
        params={"fields": "columns"},
    )
    if resp.status_code != 200:
        log.warning("Cannot fetch table %s: %d", fqn, resp.status_code)
        return None
    return [
        {"name": c["name"], "dataType": c.get("dataType", ""), "nullable": c.get("nullable", True)}
        for c in resp.json().get("columns", [])
    ]


def _detect_changes(baseline: list[dict], current: list[dict]) -> list[str]:
    issues: list[str] = []
    base_map = {c["name"]: c for c in baseline}
    curr_map = {c["name"]: c for c in current}

    for name, col in base_map.items():
        if name not in curr_map:
            issues.append(f"BREAKING: Column '{name}' was removed.")
        elif curr_map[name]["dataType"] != col["dataType"]:
            issues.append(
                f"BREAKING: Column '{name}' type changed "
                f"{col['dataType']} → {curr_map[name]['dataType']}."
            )

    new_cols = [n for n in curr_map if n not in base_map]
    for name in new_cols:
        issues.append(f"INFO: New column added: '{name}' ({curr_map[name]['dataType']}).")

    return issues


def check_table(fqn: str, snapshot_dir: Path) -> bool:
    """Returns True if breaking changes were detected."""
    current = _fetch_columns(fqn)
    if current is None:
        return False

    snap_path = _snapshot_path(snapshot_dir, fqn)

    if not snap_path.exists():
        snap_path.write_text(json.dumps(current, indent=2))
        log.info("Baseline snapshot saved for %s", fqn)
        return False

    baseline = json.loads(snap_path.read_text())
    issues = _detect_changes(baseline, current)

    if not issues:
        log.info("No schema changes detected for %s", fqn)
        # Update snapshot with latest
        snap_path.write_text(json.dumps(current, indent=2))
        return False

    breaking = [i for i in issues if i.startswith("BREAKING")]
    for issue in issues:
        if issue.startswith("BREAKING"):
            log.error("[%s] %s", fqn, issue)
        else:
            log.info("[%s] %s", fqn, issue)

    if breaking:
        # Update snapshot only if no breaking changes (preserve baseline for re-inspection)
        log.warning("Baseline snapshot NOT updated for %s due to breaking changes.", fqn)
        return True

    snap_path.write_text(json.dumps(current, indent=2))
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect breaking schema changes.")
    parser.add_argument("--snapshot-dir", default="/tmp/schema-snapshots",
                        help="Directory to store baseline snapshots.")
    args = parser.parse_args()

    snapshot_dir = Path(args.snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    breaking_detected = False
    for fqn in WATCH_TABLES:
        if check_table(fqn, snapshot_dir):
            breaking_detected = True

    if breaking_detected:
        log.error("Breaking schema changes detected — review required before deployment.")
        raise SystemExit(1)

    log.info("Schema change detection complete — no breaking changes.")


if __name__ == "__main__":
    main()
