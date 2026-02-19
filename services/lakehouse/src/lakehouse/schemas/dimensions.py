"""Dimension table Iceberg schemas — SCD Type 1 and Type 2."""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

# SCD Type 2: tracks history of symbol attributes
DIM_SYMBOL_SCHEMA = Schema(
    NestedField(1, "symbol_key", IntegerType(), required=True),
    NestedField(2, "symbol", StringType(), required=True),
    NestedField(3, "company_name", StringType(), required=True),
    NestedField(4, "sector", StringType(), required=True),
    NestedField(5, "market_cap_category", StringType(), required=True),
    NestedField(6, "effective_date", DateType(), required=True),
    NestedField(7, "expiry_date", DateType(), required=True),
    NestedField(8, "is_current", BooleanType(), required=True),
    NestedField(9, "row_hash", StringType(), required=True),
    NestedField(10, "_updated_at", TimestamptzType(), required=True),
)

# SCD Type 1: pre-populated time dimension (1 row per minute for a day)
DIM_TIME_SCHEMA = Schema(
    NestedField(1, "time_key", IntegerType(), required=True),
    NestedField(2, "hour", IntegerType(), required=True),
    NestedField(3, "minute", IntegerType(), required=True),
    NestedField(4, "time_of_day", StringType(), required=True),
    NestedField(5, "trading_session", StringType(), required=True),
    NestedField(6, "is_market_hours", BooleanType(), required=True),
)
