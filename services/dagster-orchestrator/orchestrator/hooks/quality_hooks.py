"""Quality hooks — failure alert for data quality ops."""

import logging
import os

from dagster import HookContext, failure_hook

logger = logging.getLogger(__name__)


@failure_hook(name="quality_failure_alert")
def quality_failure_alert(context: HookContext) -> None:
    """Log quality check failure and send alert if SLACK_WEBHOOK_URL is configured."""
    op_name = context.op.name if context.op else "unknown"
    context.log.error("QUALITY ALERT: Op '%s' failed quality validation.", op_name)

    slack_url = os.environ.get("SLACK_WEBHOOK_URL")
    if slack_url:
        try:
            import json
            import urllib.request

            payload = json.dumps(
                {"text": f"Data quality op FAILED: {op_name}"}
            ).encode()
            req = urllib.request.Request(
                slack_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            context.log.info("Quality alert sent to Slack for op '%s'.", op_name)
        except Exception:
            context.log.warning("Failed to send Slack quality alert for op '%s'.", op_name)
    else:
        context.log.info(
            "No SLACK_WEBHOOK_URL configured — quality failure alert logged only."
        )
