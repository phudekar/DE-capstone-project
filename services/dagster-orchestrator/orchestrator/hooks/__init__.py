"""Hook definitions for alerting and notifications."""

from orchestrator.hooks.alerting import slack_alert_on_failure

__all__ = ["slack_alert_on_failure"]
