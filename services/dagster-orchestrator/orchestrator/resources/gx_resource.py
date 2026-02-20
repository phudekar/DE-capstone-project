"""GX resource — wraps data_quality.runner.validate_dataframe for Dagster."""

import pandas as pd
from dagster import ConfigurableResource
from great_expectations.core import ExpectationSuite

from data_quality.runner import validate_dataframe


class GXResource(ConfigurableResource):
    """Dagster resource providing Great Expectations DataFrame validation."""

    def validate(self, df: pd.DataFrame, suite: ExpectationSuite) -> dict:
        """Validate a DataFrame against an ExpectationSuite.

        Returns:
            dict with keys: success (bool), statistics (dict), results (list).
        """
        return validate_dataframe(df, suite)
