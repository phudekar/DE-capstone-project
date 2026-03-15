"""Time-based partition definitions for asset scheduling."""

from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date="2026-03-13", end_offset=1)

monthly_partitions = MonthlyPartitionsDefinition(start_date="2026-03-01")
