"""create_databases.py
Register DuckDB (Iceberg lakehouse) as a database connection in Superset.

The DuckDB connection is configured with:
  - iceberg + httpfs extensions preloaded on every connect
  - S3/MinIO credentials for accessing Iceberg Parquet files
  - DML enabled for extension installation and view creation
"""

import json
import logging
import os

log = logging.getLogger(__name__)


def create_databases(superset_url: str, session) -> None:
    databases = [
        {
            "database_name": "Trade Lakehouse (DuckDB/Iceberg)",
            "sqlalchemy_uri": "duckdb:///:memory:",
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            "extra": json.dumps(
                {
                    "engine_params": {
                        "connect_args": {
                            "preload_extensions": ["iceberg", "httpfs"],
                            "config": {
                                "threads": "4",
                                "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", "2GB"),
                                "autoinstall_known_extensions": "true",
                                "autoload_known_extensions": "true",
                                "s3_access_key_id": os.environ.get("MINIO_ROOT_USER", "minio"),
                                "s3_secret_access_key": os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
                                "s3_endpoint": os.environ.get("MINIO_ENDPOINT", "minio:9000"),
                                "s3_url_style": "path",
                                "s3_use_ssl": "false",
                                "s3_region": "us-east-1",
                                "unsafe_enable_version_guessing": "true",
                            },
                        }
                    },
                    "allows_virtual_table_explore": True,
                }
            ),
            "impersonate_user": False,
        },
    ]

    for db in databases:
        resp = session.post(
            f"{superset_url}/api/v1/database/",
            json=db,
        )
        if resp.status_code in (200, 201):
            log.info("Created database: %s", db["database_name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Database already exists: %s", db["database_name"])
        else:
            log.error(
                "Failed to create database %s: %d %s",
                db["database_name"],
                resp.status_code,
                resp.text[:300],
            )
