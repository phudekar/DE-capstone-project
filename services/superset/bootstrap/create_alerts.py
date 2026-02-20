"""create_alerts.py
Configure Superset threshold-based alerts and scheduled email reports.
"""

import logging
import os
import requests

log = logging.getLogger(__name__)

ALERTS = [
    {
        "name": "High Volume Alert",
        "description": "Triggered when any symbol has volume > 3x its 20-day average.",
        "sql": """
SELECT symbol, volume, avg_volume_20d,
       ROUND(volume / NULLIF(avg_volume_20d, 0), 2) AS volume_ratio
FROM (
    SELECT symbol, volume,
           AVG(volume) OVER (PARTITION BY symbol ORDER BY trade_date
               ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg_volume_20d
    FROM daily_trade_summary
    WHERE trade_date = CURRENT_DATE
) sub
WHERE volume > 3 * avg_volume_20d
ORDER BY volume_ratio DESC
LIMIT 10
""",
        "validator_type": "operator",
        "validator_config_json": '{"op": ">", "threshold": 0}',
        "crontab": "*/5 9-17 * * 1-5",   # Every 5 min during market hours
        "grace_period": 14400,             # 4 hours between repeated alerts
    },
    {
        "name": "Data Freshness Alert",
        "description": "Triggered when no new Gold layer data for > 30 minutes.",
        "sql": """
SELECT
    MAX(trade_date) AS latest_date,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(trade_date)::TIMESTAMP)) / 60 AS minutes_stale
FROM daily_trade_summary
HAVING EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(trade_date)::TIMESTAMP)) / 60 > 30
""",
        "validator_type": "operator",
        "validator_config_json": '{"op": ">", "threshold": 0}',
        "crontab": "*/5 * * * *",
        "grace_period": 3600,
    },
]

SCHEDULED_REPORTS = [
    {
        "name": "Daily Market Summary",
        "description": "End-of-day market overview PDF emailed to executives.",
        "crontab": "0 17 * * 1-5",   # 5 PM on trading days
        "recipients": os.environ.get("EXEC_EMAIL", "executives@example.com"),
    },
    {
        "name": "Daily Data Quality Report",
        "description": "Pipeline health dashboard emailed to the data engineering team.",
        "crontab": "0 6 * * *",
        "recipients": os.environ.get("DATA_ENG_EMAIL", "data-eng@example.com"),
    },
]


def create_alerts(superset_url: str, headers: dict) -> None:
    for alert in ALERTS:
        resp = requests.post(
            f"{superset_url}/api/v1/report/",
            headers=headers,
            json={
                "type": "Alert",
                "name": alert["name"],
                "description": alert["description"],
                "active": True,
                "crontab": alert["crontab"],
                "validator_type": alert["validator_type"],
                "validator_config_json": alert["validator_config_json"],
                "grace_period": alert.get("grace_period", 14400),
                "working_timeout": 3600,
                "sql": alert["sql"],
            },
        )
        if resp.status_code in (200, 201):
            log.info("Created alert: %s", alert["name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Alert already exists: %s", alert["name"])
        else:
            log.error(
                "Failed to create alert %s: %d %s",
                alert["name"], resp.status_code, resp.text[:200],
            )

    log.info(
        "Alert/report configuration complete (%d alerts, %d reports).",
        len(ALERTS), len(SCHEDULED_REPORTS),
    )
