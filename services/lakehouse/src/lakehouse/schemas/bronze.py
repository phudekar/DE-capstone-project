"""Bronze layer Iceberg schemas — raw landing from Kafka."""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Bronze raw_trades: mirrors TradeExecuted Avro + Kafka metadata
RAW_TRADES_SCHEMA = Schema(
    NestedField(1, "trade_id", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=True),
    NestedField(3, "price", DoubleType(), required=True),
    NestedField(4, "quantity", IntegerType(), required=True),
    NestedField(5, "buyer_order_id", StringType(), required=True),
    NestedField(6, "seller_order_id", StringType(), required=True),
    NestedField(7, "buyer_agent_id", StringType(), required=True),
    NestedField(8, "seller_agent_id", StringType(), required=True),
    NestedField(9, "aggressor_side", StringType(), required=True),
    NestedField(10, "event_type", StringType(), required=True),
    NestedField(11, "timestamp", TimestamptzType(), required=True),
    # Kafka metadata
    NestedField(12, "_kafka_topic", StringType(), required=True),
    NestedField(13, "_kafka_partition", IntegerType(), required=True),
    NestedField(14, "_kafka_offset", LongType(), required=True),
    NestedField(15, "_ingested_at", TimestamptzType(), required=True),
)

# Bronze raw_orderbook: mirrors OrderBookSnapshot Avro + Kafka metadata
# Bids/asks stored as JSON strings (array of {price, quantity})
RAW_ORDERBOOK_SCHEMA = Schema(
    NestedField(1, "symbol", StringType(), required=True),
    NestedField(2, "bids_json", StringType(), required=True),
    NestedField(3, "asks_json", StringType(), required=True),
    NestedField(4, "sequence_number", IntegerType(), required=True),
    NestedField(5, "event_type", StringType(), required=True),
    NestedField(6, "timestamp", TimestamptzType(), required=True),
    # Kafka metadata
    NestedField(7, "_kafka_topic", StringType(), required=True),
    NestedField(8, "_kafka_partition", IntegerType(), required=True),
    NestedField(9, "_kafka_offset", LongType(), required=True),
    NestedField(10, "_ingested_at", TimestamptzType(), required=True),
)
