"""Tests for schedule definitions."""

from __future__ import annotations

from dagster import DefaultScheduleStatus

from orchestrator.schedules.daily import daily_gold_schedule, silver_micro_batch_schedule
from orchestrator.schedules.monthly import monthly_recompute_schedule
from orchestrator.schedules.weekly import weekly_dimension_schedule, weekly_maintenance_schedule


def test_silver_micro_batch_schedule_cron():
    """Silver micro-batch runs every 5 minutes."""
    assert silver_micro_batch_schedule.cron_schedule == "*/5 * * * *"


def test_daily_gold_schedule_cron():
    """Gold schedule runs weekdays at 5 PM UTC."""
    assert daily_gold_schedule.cron_schedule == "0 17 * * 1-5"


def test_weekly_dimension_schedule_cron():
    """Dimension refresh runs Sunday at 2 AM UTC."""
    assert weekly_dimension_schedule.cron_schedule == "0 2 * * 0"


def test_weekly_maintenance_schedule_cron():
    """Maintenance runs Sunday at 4 AM UTC."""
    assert weekly_maintenance_schedule.cron_schedule == "0 4 * * 0"


def test_monthly_recompute_schedule_cron():
    """Monthly recompute runs 1st of month at 3 AM UTC."""
    assert monthly_recompute_schedule.cron_schedule == "0 3 1 * *"


def test_all_schedules_default_stopped():
    """All schedules default to STOPPED status."""
    schedules = [
        silver_micro_batch_schedule,
        daily_gold_schedule,
        weekly_dimension_schedule,
        weekly_maintenance_schedule,
        monthly_recompute_schedule,
    ]
    for schedule in schedules:
        assert schedule.default_status == DefaultScheduleStatus.STOPPED, (
            f"{schedule.name} should default to STOPPED"
        )


def test_silver_micro_batch_schedule_name():
    """Silver micro-batch schedule has correct name."""
    assert silver_micro_batch_schedule.name == "silver_micro_batch_schedule"


def test_daily_gold_schedule_name():
    """Daily gold schedule has correct name."""
    assert daily_gold_schedule.name == "daily_gold_schedule"


def test_monthly_recompute_schedule_name():
    """Monthly recompute schedule has correct name."""
    assert monthly_recompute_schedule.name == "monthly_recompute_schedule"
