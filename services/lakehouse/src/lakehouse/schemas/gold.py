"""Gold layer Iceberg schemas — pre-aggregated business metrics."""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

DAILY_TRADING_SUMMARY_SCHEMA = Schema(
    NestedField(1, "symbol", StringType(), required=True),
    NestedField(2, "trading_date", DateType(), required=True),
    NestedField(3, "open_price", DoubleType(), required=True),
    NestedField(4, "close_price", DoubleType(), required=True),
    NestedField(5, "high_price", DoubleType(), required=True),
    NestedField(6, "low_price", DoubleType(), required=True),
    NestedField(7, "vwap", DoubleType(), required=True),
    NestedField(8, "total_volume", LongType(), required=True),
    NestedField(9, "trade_count", IntegerType(), required=True),
    NestedField(10, "total_value", DoubleType(), required=True),
    # Enrichment
    NestedField(11, "company_name", StringType(), required=False),
    NestedField(12, "sector", StringType(), required=False),
    # Metadata
    NestedField(13, "_aggregated_at", TimestamptzType(), required=True),
)
