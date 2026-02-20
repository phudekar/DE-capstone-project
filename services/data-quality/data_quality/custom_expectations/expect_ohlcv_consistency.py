"""Custom MulticolumnMapExpectation: OHLCV consistency check.

Validates that for each row:
  - high_price >= open_price
  - high_price >= close_price
  - low_price <= open_price
  - low_price <= close_price
  - vwap is between low_price and high_price (inclusive)
"""

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (
    multicolumn_condition_partial,
)


class MulticolumnValuesOhlcvConsistent(MulticolumnMapMetricProvider):
    """Metric provider for OHLCV consistency."""

    condition_metric_name = "multicolumn_values.ohlcv_consistent"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ()

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        open_p = column_list["open_price"]
        close_p = column_list["close_price"]
        high_p = column_list["high_price"]
        low_p = column_list["low_price"]
        vwap = column_list["vwap"]

        return (
            (high_p >= open_p)
            & (high_p >= close_p)
            & (low_p <= open_p)
            & (low_p <= close_p)
            & (vwap >= low_p)
            & (vwap <= high_p)
        )


class ExpectMulticolumnValuesToBeOhlcvConsistent(MulticolumnMapExpectation):
    """Expect OHLCV columns to be internally consistent."""

    map_metric = "multicolumn_values.ohlcv_consistent"
    expectation_type = "expect_multicolumn_values_to_be_ohlcv_consistent"

    default_kwarg_values = {
        **MulticolumnMapExpectation.default_kwarg_values,
        "column_list": [
            "open_price",
            "close_price",
            "high_price",
            "low_price",
            "vwap",
        ],
    }
