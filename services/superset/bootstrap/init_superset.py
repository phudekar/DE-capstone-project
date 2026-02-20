"""init_superset.py
Orchestrates the full Superset bootstrap process in dependency order.

Run once after the Superset web server is healthy:
    python bootstrap/init_superset.py

Requires:
    SUPERSET_URL   (default: http://localhost:8088)
    SUPERSET_USER  (default: admin)
    SUPERSET_PASS  (default: admin)
"""

import os
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")


def get_access_token() -> str:
    import requests
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def run_bootstrap() -> None:
    log.info("Obtaining Superset access token...")
    token = get_access_token()
    log.info("Access token obtained.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Import bootstrap modules after token acquisition
    from bootstrap.create_databases import create_databases
    from bootstrap.create_datasets import create_datasets
    from bootstrap.create_roles import create_roles
    from bootstrap.create_saved_queries import create_saved_queries
    from bootstrap.create_alerts import create_alerts

    log.info("Step 1/5: Creating database connections...")
    create_databases(SUPERSET_URL, headers)

    log.info("Step 2/5: Creating datasets...")
    create_datasets(SUPERSET_URL, headers)

    log.info("Step 3/5: Configuring roles and permissions...")
    create_roles(SUPERSET_URL, headers)

    log.info("Step 4/5: Importing saved SQL queries...")
    create_saved_queries(SUPERSET_URL, headers)

    log.info("Step 5/5: Configuring alerts and report schedules...")
    create_alerts(SUPERSET_URL, headers)

    log.info("Bootstrap complete. Open %s to access Superset.", SUPERSET_URL)


if __name__ == "__main__":
    run_bootstrap()
