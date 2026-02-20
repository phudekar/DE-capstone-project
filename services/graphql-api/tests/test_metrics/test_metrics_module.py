"""Tests for the Prometheus metrics module and MetricsExtension."""

import pytest
from prometheus_client import REGISTRY


def _get_sample_value(sample_name: str, labels: dict | None = None) -> float | None:
    """Search REGISTRY for a sample by its full sample name (e.g. foo_total)."""
    for metric in REGISTRY.collect():
        for sample in metric.samples:
            if sample.name == sample_name:
                if labels is None or all(
                    sample.labels.get(k) == v for k, v in labels.items()
                ):
                    return sample.value
    return None


def test_metrics_module_imports():
    """All metric objects should be importable without error."""
    from app.metrics import (
        graphql_requests_total,
        graphql_request_duration_seconds,
        graphql_errors_total,
        graphql_active_requests,
        graphql_cache_hits_total,
        graphql_cache_misses_total,
    )
    assert graphql_requests_total is not None
    assert graphql_request_duration_seconds is not None
    assert graphql_errors_total is not None
    assert graphql_active_requests is not None
    assert graphql_cache_hits_total is not None
    assert graphql_cache_misses_total is not None


def test_metrics_extension_imports():
    """MetricsExtension should be importable."""
    from app.extensions.metrics_extension import MetricsExtension
    assert MetricsExtension is not None


def test_counter_increments():
    """Cache counters should be incrementable."""
    from app.metrics import graphql_cache_hits_total, graphql_cache_misses_total

    before_hits = _get_sample_value("graphql_cache_hits_total") or 0.0
    before_misses = _get_sample_value("graphql_cache_misses_total") or 0.0

    graphql_cache_hits_total.inc()
    graphql_cache_misses_total.inc()

    after_hits = _get_sample_value("graphql_cache_hits_total") or 0.0
    after_misses = _get_sample_value("graphql_cache_misses_total") or 0.0

    assert after_hits == before_hits + 1
    assert after_misses == before_misses + 1


def test_active_requests_gauge():
    """Active requests gauge should support inc/dec."""
    from app.metrics import graphql_active_requests

    before = _get_sample_value("graphql_active_requests") or 0.0

    graphql_active_requests.inc()
    after_inc = _get_sample_value("graphql_active_requests") or 0.0
    assert after_inc == before + 1.0

    graphql_active_requests.dec()
    after_dec = _get_sample_value("graphql_active_requests") or 0.0
    assert after_dec == before


def test_request_duration_histogram_observe():
    """Histogram should accept observations."""
    from app.metrics import graphql_request_duration_seconds

    before = _get_sample_value(
        "graphql_request_duration_seconds_count",
        {"operation_type": "query", "operation_name": "hist_test"},
    ) or 0.0

    graphql_request_duration_seconds.labels(
        operation_type="query", operation_name="hist_test"
    ).observe(0.1)

    after = _get_sample_value(
        "graphql_request_duration_seconds_count",
        {"operation_type": "query", "operation_name": "hist_test"},
    )
    assert after is not None
    assert after == before + 1.0


def test_metrics_endpoint_returns_text():
    """The /metrics endpoint should return Prometheus text format."""
    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as client:
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert "python_" in resp.text or "graphql_" in resp.text
