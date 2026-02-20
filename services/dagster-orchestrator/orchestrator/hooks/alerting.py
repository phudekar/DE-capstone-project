"""Alerting hooks — failure notifications."""

import logging
import os

from dagster import HookContext, failure_hook

logger = logging.getLogger(__name__)


@failure_hook(name="slack_alert_on_failure")
def slack_alert_on_failure(context: HookContext) -> None:
    """Log failure details and send to Slack if SLACK_WEBHOOK_URL is configured."""
    op_name = context.op.name if context.op else "unknown"
    context.log.error("ALERT: Op '%s' failed.", op_name)

    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            import json
            import urllib.request

            payload = json.dumps(
                {"text": f"Dagster op FAILED: {op_name}"}
            ).encode()
            req = urllib.request.Request(
                slack_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            context.log.info("Slack alert sent for op '%s'.", op_name)
        except Exception:
            context.log.warning("Failed to send Slack alert for op '%s'.", op_name)
    else:
        context.log.info(
            "No SLACK_WEBHOOK_URL configured — failure alert logged only."
        )
