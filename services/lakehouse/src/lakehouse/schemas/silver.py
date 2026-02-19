"""Silver layer Iceberg schemas — cleaned, deduplicated, enriched."""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

TRADES_SCHEMA = Schema(
    NestedField(1, "trade_id", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=True),
    NestedField(3, "price", DoubleType(), required=True),
    NestedField(4, "quantity", IntegerType(), required=True),
    NestedField(5, "buyer_order_id", StringType(), required=True),
    NestedField(6, "seller_order_id", StringType(), required=True),
    NestedField(7, "buyer_agent_id", StringType(), required=True),
    NestedField(8, "seller_agent_id", StringType(), required=True),
    NestedField(9, "aggressor_side", StringType(), required=True),
    NestedField(10, "timestamp", TimestamptzType(), required=True),
    # Enrichment from dim_symbol
    NestedField(11, "company_name", StringType(), required=False),
    NestedField(12, "sector", StringType(), required=False),
    # Processing metadata
    NestedField(13, "_processed_at", TimestamptzType(), required=True),
)

ORDERBOOK_SNAPSHOTS_SCHEMA = Schema(
    NestedField(1, "symbol", StringType(), required=True),
    NestedField(2, "timestamp", TimestamptzType(), required=True),
    NestedField(3, "best_bid_price", DoubleType(), required=False),
    NestedField(4, "best_bid_qty", IntegerType(), required=False),
    NestedField(5, "best_ask_price", DoubleType(), required=False),
    NestedField(6, "best_ask_qty", IntegerType(), required=False),
    NestedField(7, "bid_depth", IntegerType(), required=True),
    NestedField(8, "ask_depth", IntegerType(), required=True),
    NestedField(9, "spread", DoubleType(), required=False),
    NestedField(10, "mid_price", DoubleType(), required=False),
    NestedField(11, "sequence_number", IntegerType(), required=True),
    # Enrichment
    NestedField(12, "company_name", StringType(), required=False),
    NestedField(13, "sector", StringType(), required=False),
    # Processing metadata
    NestedField(14, "_processed_at", TimestamptzType(), required=True),
)
