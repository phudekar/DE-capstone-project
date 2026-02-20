"""Tests for the GX-based data_quality_job."""

from orchestrator.jobs.data_quality import (
    compute_quality_scores,
    data_quality_job,
    validate_bronze,
    validate_gold,
    validate_silver,
)


def test_data_quality_job_exists():
    """data_quality_job is defined."""
    assert data_quality_job.name == "data_quality_job"


def test_data_quality_job_has_all_ops():
    """data_quality_job contains all four GX ops."""
    op_names = {node.name for node in data_quality_job.nodes}
    assert "validate_bronze" in op_names
    assert "validate_silver" in op_names
    assert "validate_gold" in op_names
    assert "compute_quality_scores" in op_names


def test_validate_bronze_op_defined():
    """validate_bronze op is correctly defined."""
    assert validate_bronze.name == "validate_bronze"
    assert validate_bronze.description is not None


def test_validate_silver_op_defined():
    """validate_silver op is correctly defined."""
    assert validate_silver.name == "validate_silver"


def test_validate_gold_op_defined():
    """validate_gold op is correctly defined."""
    assert validate_gold.name == "validate_gold"


def test_compute_quality_scores_op_defined():
    """compute_quality_scores op is correctly defined."""
    assert compute_quality_scores.name == "compute_quality_scores"
