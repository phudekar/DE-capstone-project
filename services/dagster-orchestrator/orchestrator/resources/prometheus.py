"""Prometheus resource stub — logs metrics instead of pushing to a gateway."""

from __future__ import annotations

import logging

from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class PrometheusResource(ConfigurableResource):
    """Stub Prometheus resource that logs metrics rather than pushing."""

    pushgateway_url: str = ""

    def push_metric(self, metric_name: str, value: float, labels: dict | None = None) -> None:
        """Log a metric value. No actual pushgateway interaction."""
        label_str = f" {labels}" if labels else ""
        logger.info("METRIC %s = %.4f%s", metric_name, value, label_str)
