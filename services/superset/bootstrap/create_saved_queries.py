"""create_saved_queries.py
Import pre-built SQL queries into Superset SQL Lab.
"""

import logging
import os
import requests

log = logging.getLogger(__name__)

_QUERIES_DIR = os.path.join(os.path.dirname(__file__), "..", "saved_queries")

SAVED_QUERIES = [
    {
        "label": "OHLCV with Technical Indicators (SMA + RSI)",
        "description": "Daily OHLCV data with 20-day and 50-day SMA and RSI(14) for a given symbol.",
        "file": "ohlcv_with_technicals.sql",
        "tags": ["technical-analysis", "daily", "ohlcv"],
    },
    {
        "label": "Unusual Volume Detection",
        "description": "Identifies symbols with volume > 2x their 20-day average on the current day.",
        "file": "unusual_volume_detection.sql",
        "tags": ["anomaly", "volume", "screening"],
    },
    {
        "label": "Sector Rotation Analysis",
        "description": "Weekly sector net inflow/outflow and average return over the trailing 90 days.",
        "file": "sector_rotation_analysis.sql",
        "tags": ["sector", "rotation", "flow-analysis"],
    },
    {
        "label": "Trader Profile Deep Dive",
        "description": "Comprehensive trading profile for a specific trader over a date range.",
        "file": "trader_profile_deep_dive.sql",
        "tags": ["trader", "profiling", "compliance"],
    },
    {
        "label": "Cross-Account Activity (Wash Trade Detection)",
        "description": "Detects pairs of accounts making opposite trades in the same symbol within 30 seconds.",
        "file": "cross_account_activity.sql",
        "tags": ["compliance", "wash-trade", "surveillance"],
    },
    {
        "label": "Daily Market Summary",
        "description": "Market-wide daily statistics: total trades, volume, breadth, top performers.",
        "file": "daily_market_summary.sql",
        "tags": ["market", "daily", "summary"],
    },
]


def _get_database_id(superset_url: str, headers: dict) -> int | None:
    resp = requests.get(
        f"{superset_url}/api/v1/database/",
        headers=headers,
        params={"q": '{"filters":[{"col":"database_name","opr":"ct","val":"DuckDB"}]}'},
    )
    if resp.status_code == 200:
        results = resp.json().get("result", [])
        if results:
            return results[0]["id"]
    return None


def create_saved_queries(superset_url: str, headers: dict) -> None:
    db_id = _get_database_id(superset_url, headers)
    if not db_id:
        log.error("DuckDB database not found — skipping saved queries.")
        return

    for query in SAVED_QUERIES:
        sql_path = os.path.join(_QUERIES_DIR, query["file"])
        try:
            with open(sql_path) as f:
                sql = f.read()
        except FileNotFoundError:
            log.warning("SQL file not found: %s", sql_path)
            continue

        payload = {
            "label": query["label"],
            "description": query["description"],
            "sql": sql,
            "db_id": db_id,
        }
        resp = requests.post(
            f"{superset_url}/api/v1/saved_query/",
            headers=headers,
            json=payload,
        )
        if resp.status_code in (200, 201):
            log.info("Created saved query: %s", query["label"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Saved query already exists: %s", query["label"])
        else:
            log.error(
                "Failed to create saved query %s: %d %s",
                query["label"], resp.status_code, resp.text[:200],
            )
