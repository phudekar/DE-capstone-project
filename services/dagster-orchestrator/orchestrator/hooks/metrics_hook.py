"""Dagster hooks that push Prometheus metrics on run success/failure."""

import time

from dagster import HookContext, failure_hook, success_hook

from orchestrator.metrics.dagster_metrics import (
    dagster_run_duration_seconds,
    dagster_run_failure_total,
    dagster_run_success_total,
    push_metrics,
)


@success_hook
def on_run_success(context: HookContext):
    """Push success counter and duration when an op succeeds."""
    job_name = context.op.name
    start_time = getattr(context.run, "start_time", None)
    if start_time is not None:
        duration = time.time() - start_time.timestamp()
        dagster_run_duration_seconds.labels(job_name=job_name).observe(duration)
    dagster_run_success_total.labels(job_name=job_name).inc()
    push_metrics(job_label=f"dagster_{job_name}")


@failure_hook
def on_run_failure(context: HookContext):
    """Push failure counter when an op fails."""
    job_name = context.op.name
    dagster_run_failure_total.labels(job_name=job_name).inc()
    push_metrics(job_label=f"dagster_{job_name}")
