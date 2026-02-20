"""Gold layer expectation suite for daily_trading_summary."""

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

GOLD_EXPECTED_COLUMNS = [
    "symbol",
    "trading_date",
    "open_price",
    "close_price",
    "high_price",
    "low_price",
    "vwap",
    "total_volume",
    "trade_count",
    "total_value",
    "company_name",
    "sector",
    "_aggregated_at",
]


def build_gold_daily_summary_suite() -> ExpectationSuite:
    """Build an ExpectationSuite for gold.daily_trading_summary."""
    suite = ExpectationSuite(expectation_suite_name="gold_daily_summary_suite")

    # Schema: expected columns present
    for col in GOLD_EXPECTED_COLUMNS:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
            )
        )

    # Completeness: aggregated columns NOT NULL
    for col in [
        "symbol",
        "trading_date",
        "open_price",
        "close_price",
        "high_price",
        "low_price",
        "vwap",
        "total_volume",
        "trade_count",
    ]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
            )
        )

    # Accuracy: trade_count >= 1
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "trade_count", "min_value": 1},
        )
    )

    # Accuracy: total_volume >= 1
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_volume", "min_value": 1},
        )
    )

    # OHLCV: high_price >= open_price and close_price (custom expectation)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_multicolumn_values_to_be_ohlcv_consistent",
            kwargs={
                "column_list": [
                    "open_price",
                    "close_price",
                    "high_price",
                    "low_price",
                    "vwap",
                ],
            },
        )
    )

    return suite
