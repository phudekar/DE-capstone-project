"""init_superset.py
Orchestrates the full Superset bootstrap process in dependency order.

Run once after the Superset web server is healthy:
    python bootstrap/init_superset.py

Requires:
    SUPERSET_URL   (default: http://localhost:8088)
    SUPERSET_USER  (default: admin)
    SUPERSET_PASS  (default: admin)
"""

import logging
import os

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")


def create_session() -> requests.Session:
    """Create an authenticated session with JWT + CSRF tokens."""
    session = requests.Session()

    # Get JWT access token
    resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]

    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
    )

    # Get CSRF token (session cookies are set automatically)
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    resp.raise_for_status()
    csrf_token = resp.json()["result"]

    session.headers.update(
        {
            "X-CSRFToken": csrf_token,
            "Referer": SUPERSET_URL,
        }
    )

    return session


def run_bootstrap() -> None:
    log.info("Obtaining Superset access token + CSRF token...")
    session = create_session()
    log.info("Authenticated session established.")

    from bootstrap.create_alerts import create_alerts
    from bootstrap.create_charts import create_charts
    from bootstrap.create_databases import create_databases
    from bootstrap.create_datasets import create_datasets
    from bootstrap.create_roles import create_roles
    from bootstrap.create_saved_queries import create_saved_queries
    from bootstrap.create_symbol_dashboard import create_symbol_dashboard

    log.info("Step 1/7: Creating database connections...")
    create_databases(SUPERSET_URL, session)

    log.info("Step 2/7: Creating datasets...")
    create_datasets(SUPERSET_URL, session)

    log.info("Step 3/7: Configuring roles and permissions...")
    create_roles(SUPERSET_URL, session)

    log.info("Step 4/7: Importing saved SQL queries...")
    create_saved_queries(SUPERSET_URL, session)

    log.info("Step 5/7: Configuring alerts and report schedules...")
    create_alerts(SUPERSET_URL, session)

    log.info("Step 6/7: Creating charts...")
    create_charts(SUPERSET_URL, session)

    log.info("Step 7/7: Creating Symbol Analysis dashboard...")
    create_symbol_dashboard(SUPERSET_URL, session)

    log.info("Bootstrap complete. Open %s to access Superset.", SUPERSET_URL)


if __name__ == "__main__":
    run_bootstrap()
