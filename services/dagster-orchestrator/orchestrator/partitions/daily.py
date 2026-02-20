"""Time-based partition definitions for asset scheduling."""

from dagster import DailyPartitionsDefinition, MonthlyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

monthly_partitions = MonthlyPartitionsDefinition(start_date="2024-01-01")
