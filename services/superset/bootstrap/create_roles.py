"""create_roles.py
Configure Superset roles: Viewer, Analyst, DataEngineer, Compliance.
"""

import logging
import requests

log = logging.getLogger(__name__)

ROLES = [
    {
        "name": "TradeViewer",
        "permissions": [
            "can_read on Dashboard",
            "can_read on Chart",
            "can_read on Dataset",
            "menu_access on Dashboards",
        ],
    },
    {
        "name": "TradeAnalyst",
        "permissions": [
            "can_read on Dashboard",
            "can_read on Chart",
            "can_read on Dataset",
            "can_write on SavedQuery",
            "can_read on Query",
            "can_sql_lab on Superset",
            "menu_access on Dashboards",
            "menu_access on SQL Lab",
        ],
    },
    {
        "name": "DataEngineer",
        "permissions": [
            "can_read on Dashboard",
            "can_write on Dashboard",
            "can_read on Chart",
            "can_write on Chart",
            "can_read on Dataset",
            "can_write on Dataset",
            "can_write on SavedQuery",
            "can_sql_lab on Superset",
            "can_read on Database",
            "menu_access on Dashboards",
            "menu_access on SQL Lab",
            "menu_access on Data",
        ],
    },
    {
        "name": "Compliance",
        "permissions": [
            "can_read on Dashboard",
            "can_read on Chart",
            "can_read on Dataset",
            "can_sql_lab on Superset",
            "menu_access on Dashboards",
        ],
    },
]


def _get_permission_ids(superset_url: str, headers: dict) -> dict[str, int]:
    resp = requests.get(
        f"{superset_url}/api/v1/security/permissions/",
        headers=headers,
        params={"page_size": 200},
    )
    if resp.status_code != 200:
        log.error("Failed to fetch permissions: %d", resp.status_code)
        return {}
    return {p["name"]: p["id"] for p in resp.json().get("result", [])}


def create_roles(superset_url: str, headers: dict) -> None:
    perm_map = _get_permission_ids(superset_url, headers)

    for role in ROLES:
        # Check if role already exists
        resp = requests.get(
            f"{superset_url}/api/v1/security/roles/",
            headers=headers,
            params={"q": f'{{"filters":[{{"col":"name","opr":"eq","val":"{role["name"]}"}}]}}'},
        )
        existing = resp.json().get("result", []) if resp.status_code == 200 else []

        if existing:
            log.info("Role already exists: %s", role["name"])
            continue

        # Create role
        create_resp = requests.post(
            f"{superset_url}/api/v1/security/roles/",
            headers=headers,
            json={"name": role["name"]},
        )
        if create_resp.status_code not in (200, 201):
            log.error("Failed to create role %s: %d", role["name"], create_resp.status_code)
            continue

        log.info("Created role: %s", role["name"])
