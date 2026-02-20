"""create_databases.py
Register DuckDB (Iceberg lakehouse) as a database connection in Superset.
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

# Path to the DuckDB init SQL, relative to the Superset container
INIT_SQL_PATH = "/app/pythonpath/init_duckdb.sql"


def _read_init_sql() -> str:
    local_path = os.path.join(os.path.dirname(__file__), "..", "init_duckdb.sql")
    try:
        with open(local_path) as f:
            return f.read()
    except FileNotFoundError:
        return ""


def create_databases(superset_url: str, headers: dict) -> None:
    init_sql = _read_init_sql()

    databases = [
        {
            "database_name": "Trade Lakehouse (DuckDB/Iceberg)",
            "sqlalchemy_uri": "duckdb:////tmp/trade_analytics.duckdb",
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": False,
            "extra": {
                "engine_params": {
                    "connect_args": {
                        "read_only": False,
                        "config": {
                            "threads": 4,
                            "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB"),
                        },
                    }
                },
                "allows_virtual_table_explore": True,
                "cost_estimate_enabled": True,
            },
            "impersonate_user": False,
        },
    ]

    for db in databases:
        resp = requests.post(
            f"{superset_url}/api/v1/database/",
            headers=headers,
            json=db,
        )
        if resp.status_code in (200, 201):
            log.info("Created database: %s", db["database_name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Database already exists: %s", db["database_name"])
        else:
            log.error(
                "Failed to create database %s: %d %s",
                db["database_name"], resp.status_code, resp.text[:300],
            )
