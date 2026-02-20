"""Tests for job definitions."""

from orchestrator.jobs.data_quality import (
    compute_quality_scores,
    data_quality_job,
    validate_bronze,
    validate_gold,
    validate_silver,
)
from orchestrator.jobs.export import export_daily_summary_csv, export_job
from orchestrator.jobs.maintenance import compact_tables, expire_snapshots, maintenance_job


def test_maintenance_job_exists():
    """Maintenance job is defined."""
    assert maintenance_job.name == "maintenance_job"


def test_data_quality_job_exists():
    """Data quality job is defined."""
    assert data_quality_job.name == "data_quality_job"


def test_export_job_exists():
    """Export job is defined."""
    assert export_job.name == "export_job"


def test_maintenance_job_has_ops():
    """Maintenance job contains compact_tables and expire_snapshots ops."""
    op_names = {node.name for node in maintenance_job.nodes}
    assert "compact_tables" in op_names
    assert "expire_snapshots" in op_names


def test_data_quality_job_has_ops():
    """Data quality job contains all four GX ops."""
    op_names = {node.name for node in data_quality_job.nodes}
    assert "validate_bronze" in op_names
    assert "validate_silver" in op_names
    assert "validate_gold" in op_names
    assert "compute_quality_scores" in op_names


def test_export_job_has_ops():
    """Export job contains export op."""
    op_names = {node.name for node in export_job.nodes}
    assert "export_daily_summary_csv" in op_names


def test_compact_tables_op_defined():
    """compact_tables op is defined with description."""
    assert compact_tables.name == "compact_tables"
    assert compact_tables.description is not None


def test_validate_bronze_op_defined():
    """validate_bronze op is defined."""
    assert validate_bronze.name == "validate_bronze"


def test_export_daily_summary_csv_op_defined():
    """export_daily_summary_csv op is defined."""
    assert export_daily_summary_csv.name == "export_daily_summary_csv"
