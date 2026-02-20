"""Tests for the Dagster Prometheus metrics module."""

from unittest.mock import MagicMock, patch


def test_metrics_module_imports():
    """All metric objects should import without error."""
    from orchestrator.metrics.dagster_metrics import (
        dagster_run_success_total,
        dagster_run_failure_total,
        dagster_run_duration_seconds,
        dagster_asset_materialization_total,
        dagster_asset_materialization_duration_seconds,
        dagster_asset_freshness_seconds,
        dagster_sensor_tick_total,
        dagster_sensor_evaluation_total,
        dagster_sensor_skip_total,
        dagster_schedule_tick_total,
        dagster_schedule_late_total,
        registry,
    )
    assert dagster_run_success_total is not None
    assert dagster_run_failure_total is not None
    assert dagster_run_duration_seconds is not None
    assert dagster_asset_materialization_total is not None
    assert dagster_asset_materialization_duration_seconds is not None
    assert dagster_asset_freshness_seconds is not None
    assert dagster_sensor_tick_total is not None
    assert dagster_sensor_evaluation_total is not None
    assert dagster_sensor_skip_total is not None
    assert dagster_schedule_tick_total is not None
    assert dagster_schedule_late_total is not None
    assert registry is not None


def test_counter_labels():
    """Counters should accept correct label sets."""
    from orchestrator.metrics.dagster_metrics import (
        dagster_run_success_total,
        dagster_run_failure_total,
        dagster_sensor_tick_total,
        dagster_schedule_tick_total,
    )
    # These should not raise
    dagster_run_success_total.labels(job_name="test_job").inc()
    dagster_run_failure_total.labels(job_name="test_job").inc()
    dagster_sensor_tick_total.labels(sensor_name="test_sensor").inc()
    dagster_schedule_tick_total.labels(schedule_name="test_schedule").inc()


def test_histogram_observe():
    """Histograms should accept observations."""
    from orchestrator.metrics.dagster_metrics import (
        dagster_run_duration_seconds,
        dagster_asset_materialization_duration_seconds,
    )
    dagster_run_duration_seconds.labels(job_name="test_job").observe(10.5)
    dagster_asset_materialization_duration_seconds.labels(
        asset_key="silver_trades", layer="silver"
    ).observe(5.0)


def test_gauge_set():
    """Gauges should accept set() calls."""
    from orchestrator.metrics.dagster_metrics import dagster_asset_freshness_seconds

    dagster_asset_freshness_seconds.labels(
        asset_key="gold_daily_trading_summary", layer="gold"
    ).set(3600.0)


def test_push_metrics_handles_failure_gracefully():
    """push_metrics should not raise if Pushgateway is unreachable."""
    from orchestrator.metrics.dagster_metrics import push_metrics

    with patch(
        "orchestrator.metrics.dagster_metrics.push_to_gateway",
        side_effect=Exception("connection refused"),
    ):
        # Should not raise
        push_metrics(job_label="test")


def test_push_metrics_calls_gateway():
    """push_metrics should call push_to_gateway with correct args."""
    from orchestrator.metrics.dagster_metrics import push_metrics, registry

    with patch("orchestrator.metrics.dagster_metrics.push_to_gateway") as mock_push:
        push_metrics(job_label="dagster_test")
        mock_push.assert_called_once()
        call_kwargs = mock_push.call_args
        assert call_kwargs[1]["job"] == "dagster_test" or call_kwargs[0][1] == "dagster_test"
