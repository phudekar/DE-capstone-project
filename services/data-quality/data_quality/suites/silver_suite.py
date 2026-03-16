"""Silver layer expectation suite for trades."""

import json
import os

from great_expectations.core import ExpectationConfiguration, ExpectationSuite

SILVER_EXPECTED_COLUMNS = [
    "trade_id",
    "symbol",
    "price",
    "quantity",
    "buy_order_id",
    "sell_order_id",
    "buyer_agent_id",
    "seller_agent_id",
    "is_aggressive_buy",
    "timestamp",
    "company_name",
    "sector",
    "_processed_at",
]

# Default path matches the Docker container layout; override via env var
_REFERENCE_DATA_PATH = os.getenv("REFERENCE_DATA_PATH", "/app/data/reference/symbols.json")


def _load_known_symbols() -> list[str]:
    """Load valid symbols from reference data file."""
    try:
        with open(_REFERENCE_DATA_PATH) as f:
            return [entry["symbol"] for entry in json.load(f)]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        # Fallback if reference file is unavailable
        return [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "META",
            "TSLA",
            "NVDA",
            "CRM",
            "JPM",
            "BAC",
            "GS",
            "V",
            "JNJ",
            "UNH",
            "PFE",
            "WMT",
            "KO",
            "MCD",
            "XOM",
            "CVX",
            "SMCI",
            "MSTR",
            "PLTR",
            "RIVN",
        ]


KNOWN_SYMBOLS = _load_known_symbols()


def build_silver_trades_suite(symbols: list[str] | None = None) -> ExpectationSuite:
    """Build an ExpectationSuite for silver.trades.

    Args:
        symbols: Known valid symbol set. Defaults to KNOWN_SYMBOLS.
    """
    suite = ExpectationSuite(expectation_suite_name="silver_trades_suite")
    valid_symbols = symbols or KNOWN_SYMBOLS

    # Schema: expected columns present
    for col in SILVER_EXPECTED_COLUMNS:
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": col},
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

    # Uniqueness: trade_id must be unique
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "trade_id"},
        )
    )

    # Accuracy: price in [0.01, 100000]
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "price", "min_value": 0.01, "max_value": 100000},
        )
    )

    # Accuracy: quantity in [1, 1000000]
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "quantity", "min_value": 1, "max_value": 1000000},
        )
    )

    # Referential integrity: symbol in known set
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "symbol", "value_set": valid_symbols},
        )
    )

    return suite
