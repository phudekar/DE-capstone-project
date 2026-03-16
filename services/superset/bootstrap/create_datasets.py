"""create_datasets.py
Register Iceberg tables as Superset virtual datasets using iceberg_scan().

The DuckDB connection must be configured with iceberg + httpfs extensions and
S3/MinIO credentials in connect_args (see create_databases.py).
"""

import logging

log = logging.getLogger(__name__)

# Virtual datasets use iceberg_scan() to read directly from Iceberg/MinIO
DATASETS = [
    {
        "table_name": "gold_daily_trading_summary",
        "sql": (
            "SELECT symbol, trading_date AS trade_date, "
            "open_price, close_price, high_price, low_price, "
            "vwap, total_volume, trade_count, total_value, "
            "company_name, sector, "
            "ROUND((close_price - open_price) / NULLIF(open_price, 0) * 100, 4) AS change_pct "
            "FROM iceberg_scan('s3://warehouse/gold/daily_trading_summary')"
        ),
    },
    {
        "table_name": "silver_trades",
        "sql": (
            "SELECT trade_id, symbol, price, quantity, timestamp, "
            "company_name, sector, _processed_at "
            "FROM iceberg_scan('s3://warehouse/silver/trades')"
        ),
    },
    {
        "table_name": "dim_symbols",
        "sql": (
            "SELECT symbol, company_name, sector, is_current FROM iceberg_scan('s3://warehouse/dimensions/dim_symbol')"
        ),
    },
]


def _get_database_id(superset_url: str, session) -> int | None:
    resp = session.get(f"{superset_url}/api/v1/database/")
    if resp.status_code == 200:
        for db in resp.json().get("result", []):
            if "DuckDB" in db.get("database_name", ""):
                return db["id"]
    return None


def create_datasets(superset_url: str, session) -> None:
    db_id = _get_database_id(superset_url, session)
    if not db_id:
        log.error("DuckDB database not found — run create_databases.py first.")
        return

    for ds in DATASETS:
        payload = {
            "database": db_id,
            "table_name": ds["table_name"],
            "sql": ds["sql"],
            "schema": "",
        }
        resp = session.post(
            f"{superset_url}/api/v1/dataset/",
            json=payload,
        )
        if resp.status_code in (200, 201):
            log.info("Created dataset: %s", ds["table_name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Dataset already exists: %s", ds["table_name"])
        else:
            log.error(
                "Failed to create dataset %s: %d %s",
                ds["table_name"],
                resp.status_code,
                resp.text[:300],
            )
