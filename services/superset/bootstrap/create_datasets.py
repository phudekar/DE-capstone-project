"""create_datasets.py
Register Iceberg DuckDB views as Superset datasets (virtual tables).
"""

import logging
import requests

log = logging.getLogger(__name__)

DATASETS = [
    {
        "table_name": "daily_trade_summary",
        "sql": "SELECT * FROM daily_trade_summary",
        "description": "Daily OHLCV trading summary per symbol — Gold layer aggregate.",
        "metrics": [
            {"metric_name": "total_volume", "expression": "SUM(total_volume)"},
            {"metric_name": "avg_vwap", "expression": "AVG(vwap)"},
            {"metric_name": "trade_count", "expression": "SUM(trade_count)"},
            {"metric_name": "total_value", "expression": "SUM(total_value_traded)"},
        ],
    },
    {
        "table_name": "trades_enriched",
        "sql": "SELECT * FROM trades_enriched",
        "description": "Individual trade records from the Silver layer, enriched with computed columns.",
        "metrics": [
            {"metric_name": "total_trades", "expression": "COUNT(*)"},
            {"metric_name": "total_volume", "expression": "SUM(quantity)"},
            {"metric_name": "total_value", "expression": "SUM(total_value)"},
        ],
    },
    {
        "table_name": "market_daily_overview",
        "sql": "SELECT * FROM market_daily_overview",
        "description": "Market-wide daily summary: total volume, value, breadth (advancing/declining).",
        "metrics": [
            {"metric_name": "total_trades", "expression": "SUM(total_trades)"},
            {"metric_name": "total_volume", "expression": "SUM(total_volume)"},
            {"metric_name": "total_value", "expression": "SUM(total_value_traded)"},
        ],
    },
    {
        "table_name": "symbol_reference",
        "sql": "SELECT * FROM symbol_reference",
        "description": "Symbol reference data: ticker, company name, sector.",
        "metrics": [
            {"metric_name": "symbol_count", "expression": "COUNT(DISTINCT ticker)"},
        ],
    },
    {
        "table_name": "sector_daily_summary",
        "sql": "SELECT * FROM sector_daily_summary",
        "description": "Sector-level daily aggregates: volume, avg return, total value.",
        "metrics": [
            {"metric_name": "total_volume", "expression": "SUM(total_volume)"},
            {"metric_name": "avg_change", "expression": "AVG(avg_change_pct)"},
        ],
    },
    {
        "table_name": "trader_performance_metrics",
        "sql": "SELECT * FROM trader_performance_metrics",
        "description": "Daily trader performance metrics aggregated from Silver trades.",
        "metrics": [
            {"metric_name": "total_trades", "expression": "SUM(total_trades)"},
            {"metric_name": "total_volume", "expression": "SUM(total_volume)"},
        ],
    },
    {
        "table_name": "top_movers",
        "sql": "SELECT * FROM top_movers",
        "description": "Daily top-N symbols ranked by absolute price change percentage.",
        "metrics": [
            {"metric_name": "mover_count", "expression": "COUNT(DISTINCT symbol)"},
        ],
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


def create_datasets(superset_url: str, headers: dict) -> None:
    db_id = _get_database_id(superset_url, headers)
    if not db_id:
        log.error("DuckDB database not found — run create_databases.py first.")
        return

    for ds in DATASETS:
        payload = {
            "database": db_id,
            "table_name": ds["table_name"],
            "sql": ds["sql"],
            "description": ds["description"],
        }
        resp = requests.post(
            f"{superset_url}/api/v1/dataset/",
            headers=headers,
            json=payload,
        )
        if resp.status_code in (200, 201):
            log.info("Created dataset: %s", ds["table_name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Dataset already exists: %s", ds["table_name"])
        else:
            log.error(
                "Failed to create dataset %s: %d %s",
                ds["table_name"], resp.status_code, resp.text[:300],
            )
