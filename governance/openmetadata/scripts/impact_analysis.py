"""impact_analysis.py
Query the OpenMetadata lineage API to perform downstream impact analysis.
Given an upstream asset (table or topic), prints all downstream dependents.

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python impact_analysis.py \
        --fqn "de-capstone-iceberg.iceberg.bronze.raw_trades" \
        --depth 3
"""

import os
import sys
import logging
import argparse
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585")
OM_TOKEN = os.environ.get("OM_TOKEN", "CHANGE_ME")
API = f"{OM_HOST}/api/v1"
HEADERS = {"Authorization": f"Bearer {OM_TOKEN}"}


def _get_entity_id(fqn: str, entity_type: str = "tables") -> str | None:
    resp = requests.get(
        f"{API}/{entity_type}/name/{fqn}",
        headers=HEADERS,
        params={"fields": "id"},
    )
    if resp.status_code == 200:
        return resp.json()["id"]
    log.warning("Entity not found: %s (type=%s)", fqn, entity_type)
    return None


def _get_lineage(entity_id: str, entity_type: str, depth: int, direction: str) -> dict:
    resp = requests.get(
        f"{API}/lineage/{entity_type}/{entity_id}",
        headers=HEADERS,
        params={"upstreamDepth": 0 if direction == "downstream" else depth,
                "downstreamDepth": depth if direction == "downstream" else 0},
    )
    if resp.status_code == 200:
        return resp.json()
    log.error("Lineage query failed: %d %s", resp.status_code, resp.text[:200])
    return {}


def _print_lineage(lineage: dict, root_id: str, indent: int = 0) -> None:
    edges = lineage.get("downstreamEdges", [])
    nodes = {n["id"]: n for n in lineage.get("nodes", [])}
    children = [e["toEntity"]["id"] for e in edges if e.get("fromEntity", {}).get("id") == root_id]
    for child_id in children:
        node = nodes.get(child_id, {})
        name = node.get("fullyQualifiedName") or node.get("name", child_id)
        entity_type = node.get("type", "?")
        print(f"{'  ' * indent}→ [{entity_type}] {name}")
        _print_lineage(lineage, child_id, indent + 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Downstream impact analysis via OpenMetadata.")
    parser.add_argument("--fqn", required=True, help="Fully-qualified name of the upstream asset.")
    parser.add_argument("--type", default="tables", help="Entity type (tables, topics, pipelines).")
    parser.add_argument("--depth", type=int, default=3, help="Max downstream depth.")
    args = parser.parse_args()

    entity_id = _get_entity_id(args.fqn, args.type)
    if not entity_id:
        sys.exit(1)

    log.info("Fetching downstream lineage for %s (depth=%d)...", args.fqn, args.depth)
    lineage = _get_lineage(entity_id, args.type, args.depth, "downstream")

    if not lineage:
        print("No lineage data found.")
        return

    print(f"\nDownstream impact analysis for: {args.fqn}")
    print("=" * 60)
    _print_lineage(lineage, entity_id)
    print()


if __name__ == "__main__":
    main()
