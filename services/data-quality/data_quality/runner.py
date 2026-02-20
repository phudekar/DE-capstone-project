"""GX runner — ephemeral in-memory context for validating DataFrames."""

import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def _build_context() -> BaseDataContext:
    """Create an ephemeral in-memory GX context (no YAML, no filesystem)."""
    config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )
    return BaseDataContext(project_config=config)


def validate_dataframe(
    df: pd.DataFrame,
    suite: ExpectationSuite,
    datasource_name: str = "runtime_ds",
) -> dict:
    """Validate a Pandas DataFrame against an ExpectationSuite.

    Args:
        df: The DataFrame to validate.
        suite: A GX ExpectationSuite with expectations configured.
        datasource_name: Name for the ephemeral datasource.

    Returns:
        A dict with keys: success (bool), statistics (dict), results (list).
    """
    context = _build_context()

    # Add suite to context
    context.add_expectation_suite(expectation_suite=suite)

    # Add a RuntimeDataConnector datasource
    context.add_datasource(
        datasource_name,
        class_name="Datasource",
        module_name="great_expectations.datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        data_connectors={
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["batch_id"],
            }
        },
    )

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="runtime_connector",
        data_asset_name="validation_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "default"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite.expectation_suite_name,
    )

    result = validator.validate()

    return {
        "success": result.success,
        "statistics": result.statistics,
        "results": [
            {
                "expectation_type": r.expectation_config.expectation_type,
                "success": r.success,
                "result": r.result,
            }
            for r in result.results
        ],
    }
