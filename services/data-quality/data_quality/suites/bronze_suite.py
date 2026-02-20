"""Bronze layer expectation suite for raw_trades."""

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

BRONZE_EXPECTED_COLUMNS = [
    "trade_id",
    "symbol",
    "price",
    "quantity",
    "buyer_order_id",
    "seller_order_id",
    "buyer_agent_id",
    "seller_agent_id",
    "aggressor_side",
    "event_type",
    "timestamp",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_ingested_at",
]


def build_bronze_trades_suite() -> ExpectationSuite:
    """Build an ExpectationSuite for bronze.raw_trades."""
    suite = ExpectationSuite(expectation_suite_name="bronze_trades_suite")

    # Schema: expected columns present
    for col in BRONZE_EXPECTED_COLUMNS:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
            )
        )

    # Table non-empty
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1},
        )
    )

    # Completeness: required columns NOT NULL
    for col in ["trade_id", "symbol", "price", "quantity", "timestamp"]:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": col},
            )
        )

    # Accuracy: price > 0
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "price", "min_value": 0, "strict_min": True},
        )
    )

    # Accuracy: quantity >= 1
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "quantity", "min_value": 1},
        )
    )

    # Validity: aggressor_side in {"Buy", "Sell"}
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "aggressor_side", "value_set": ["Buy", "Sell"]},
        )
    )

    return suite
