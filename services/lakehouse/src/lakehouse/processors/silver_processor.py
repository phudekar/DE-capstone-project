"""Silver processor: Bronze → Silver (deduplicate, cast, enrich)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)

TRADE_BATCH_SIZE = 50_000

# Explicit Arrow schemas matching Iceberg table definitions (required = not nullable)
SILVER_TRADES_ARROW_SCHEMA = pa.schema([
    pa.field("trade_id", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("quantity", pa.int32(), nullable=False),
    pa.field("buy_order_id", pa.string(), nullable=False),
    pa.field("sell_order_id", pa.string(), nullable=False),
    pa.field("buyer_agent_id", pa.string(), nullable=False),
    pa.field("seller_agent_id", pa.string(), nullable=False),
    pa.field("is_aggressive_buy", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("company_name", pa.string(), nullable=True),
    pa.field("sector", pa.string(), nullable=True),
    pa.field("_processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
])

SILVER_ORDERBOOK_ARROW_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("best_bid_price", pa.float64(), nullable=True),
    pa.field("best_bid_qty", pa.int32(), nullable=True),
    pa.field("best_ask_price", pa.float64(), nullable=True),
    pa.field("best_ask_qty", pa.int32(), nullable=True),
    pa.field("bid_depth", pa.int32(), nullable=False),
    pa.field("ask_depth", pa.int32(), nullable=False),
    pa.field("spread", pa.float64(), nullable=True),
    pa.field("mid_price", pa.float64(), nullable=True),
    pa.field("sequence_number", pa.int32(), nullable=False),
    pa.field("company_name", pa.string(), nullable=True),
    pa.field("sector", pa.string(), nullable=True),
    pa.field("_processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
])


def _load_dim_symbol(catalog) -> dict[str, dict]:
    """Load current dim_symbol rows keyed by symbol."""
    table = catalog.load_table(f"{config.NS_DIM}.dim_symbol")
    scan = table.scan(row_filter="is_current == true")
    arrow = scan.to_arrow()
    result = {}
    for i in range(len(arrow)):
        row = {col: arrow.column(col)[i].as_py() for col in arrow.column_names}
        result[row["symbol"]] = row
    return result


def _get_existing_trade_ids(silver_table) -> set[str]:
    """Load existing Silver trade IDs for dedup."""
    existing_arrow = silver_table.scan(selected_fields=("trade_id",)).to_arrow()
    if len(existing_arrow) > 0:
        return set(existing_arrow.column("trade_id").to_pylist())
    return set()


def _process_trades_batch(
    batch_arrow: pa.Table,
    existing_ids: set[str],
    dim_symbols: dict[str, dict],
    now: datetime,
) -> pa.Table | None:
    """Process a single batch of Bronze trades into Silver format."""
    df = batch_arrow.to_pandas()
    df = df.sort_values("_kafka_offset").drop_duplicates(subset=["trade_id"], keep="first")

    # Filter out already-processed trades
    if existing_ids:
        df = df[~df["trade_id"].isin(existing_ids)]

    if len(df) == 0:
        return None

    # Vectorized enrichment via merge instead of row-by-row loop
    dim_df = pa.table({
        "symbol": [s for s in dim_symbols],
        "company_name": [d.get("company_name") for d in dim_symbols.values()],
        "sector": [d.get("sector") for d in dim_symbols.values()],
    }).to_pandas()
    df = df.merge(dim_df, on="symbol", how="left")

    df["is_aggressive_buy"] = df["is_aggressive_buy"].astype(str)
    df["_processed_at"] = now

    silver_cols = [
        "trade_id", "symbol", "price", "quantity",
        "buy_order_id", "sell_order_id",
        "buyer_agent_id", "seller_agent_id",
        "is_aggressive_buy", "timestamp",
        "company_name", "sector", "_processed_at",
    ]
    return pa.Table.from_pandas(df[silver_cols], schema=SILVER_TRADES_ARROW_SCHEMA, preserve_index=False)


def process_trades(catalog, since: datetime | None = None) -> int:
    """Read unprocessed Bronze trades, deduplicate, enrich, write to Silver.

    Streams data via RecordBatchReader to avoid loading the entire Bronze
    table into memory at once.

    Args:
        catalog: PyIceberg catalog
        since: only process records ingested after this timestamp.
               If None, processes all Bronze records.

    Returns:
        Number of Silver records written.
    """
    bronze_table = catalog.load_table(f"{config.NS_BRONZE}.raw_trades")
    silver_table = catalog.load_table(f"{config.NS_SILVER}.trades")

    scan = bronze_table.scan()
    if since is not None:
        scan = bronze_table.scan(row_filter=f"_ingested_at > '{since.isoformat()}'")

    # Load existing IDs and dimension data once
    existing_ids = _get_existing_trade_ids(silver_table)
    dim_symbols = _load_dim_symbol(catalog)
    now = datetime.now(timezone.utc)

    total_written = 0
    reader = scan.to_arrow_batch_reader()

    # Accumulate record batches until we hit TRADE_BATCH_SIZE, then flush
    accumulated = []
    accumulated_rows = 0

    for record_batch in reader:
        if record_batch.num_rows == 0:
            continue

        accumulated.append(record_batch)
        accumulated_rows += record_batch.num_rows

        if accumulated_rows >= TRADE_BATCH_SIZE:
            batch_table = pa.Table.from_batches(accumulated, schema=reader.schema)
            accumulated = []
            accumulated_rows = 0

            silver_batch = _process_trades_batch(batch_table, existing_ids, dim_symbols, now)
            if silver_batch is not None and len(silver_batch) > 0:
                silver_table.append(silver_batch)
                batch_count = len(silver_batch)
                total_written += batch_count
                existing_ids.update(set(silver_batch.column("trade_id").to_pylist()))
                logger.info("Flushed %d Silver trade records (total: %d).", batch_count, total_written)

    # Flush remaining
    if accumulated:
        batch_table = pa.Table.from_batches(accumulated, schema=reader.schema)
        silver_batch = _process_trades_batch(batch_table, existing_ids, dim_symbols, now)
        if silver_batch is not None and len(silver_batch) > 0:
            silver_table.append(silver_batch)
            total_written += len(silver_batch)
            logger.info("Flushed %d Silver trade records (total: %d).", len(silver_batch), total_written)

    if total_written == 0:
        logger.info("No new Bronze trades to process.")
    else:
        logger.info("Wrote %d total records to silver.trades.", total_written)
    return total_written


def _extract_top_of_book(bids_json, asks_json):
    """Extract top-of-book metrics from JSON bid/ask arrays."""
    bids = json.loads(bids_json) if bids_json else []
    asks = json.loads(asks_json) if asks_json else []

    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None
    best_bid_price = best_bid["price"] if best_bid else None
    best_ask_price = best_ask["price"] if best_ask else None

    spread = None
    mid_price = None
    if best_bid_price is not None and best_ask_price is not None:
        spread = best_ask_price - best_bid_price
        mid_price = (best_bid_price + best_ask_price) / 2.0

    return {
        "best_bid_price": best_bid_price,
        "best_bid_qty": best_bid["quantity"] if best_bid else None,
        "best_ask_price": best_ask_price,
        "best_ask_qty": best_ask["quantity"] if best_ask else None,
        "bid_depth": len(bids),
        "ask_depth": len(asks),
        "spread": spread,
        "mid_price": mid_price,
    }


def _process_orderbook_batch(
    batch_arrow: pa.Table,
    dim_symbols: dict[str, dict],
    now: datetime,
) -> pa.Table | None:
    """Process a single batch of Bronze orderbook into Silver format."""
    df = batch_arrow.to_pandas()
    df = df.sort_values("_kafka_offset").drop_duplicates(
        subset=["symbol", "timestamp"], keep="first"
    )

    if len(df) == 0:
        return None

    # Extract top-of-book metrics
    tob = df.apply(
        lambda r: _extract_top_of_book(r.get("bids_json"), r.get("asks_json")),
        axis=1,
        result_type="expand",
    )
    for col in tob.columns:
        df[col] = tob[col]

    # Enrich with dim_symbol
    dim_df = pa.table({
        "symbol": [s for s in dim_symbols],
        "company_name": [d.get("company_name") for d in dim_symbols.values()],
        "sector": [d.get("sector") for d in dim_symbols.values()],
    }).to_pandas()
    df = df.merge(dim_df, on="symbol", how="left")
    df["_processed_at"] = now

    silver_cols = [
        "symbol", "timestamp",
        "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty",
        "bid_depth", "ask_depth", "spread", "mid_price",
        "sequence_number", "company_name", "sector", "_processed_at",
    ]
    return pa.Table.from_pandas(df[silver_cols], schema=SILVER_ORDERBOOK_ARROW_SCHEMA, preserve_index=False)


def process_orderbook(catalog, since: datetime | None = None) -> int:
    """Read unprocessed Bronze orderbook, extract top-of-book, enrich, write Silver.

    Streams data via RecordBatchReader to avoid loading the entire Bronze
    table into memory at once.

    Args:
        catalog: PyIceberg catalog
        since: only process records ingested after this timestamp.

    Returns:
        Number of Silver records written.
    """
    bronze_table = catalog.load_table(f"{config.NS_BRONZE}.raw_orderbook")
    silver_table = catalog.load_table(f"{config.NS_SILVER}.orderbook_snapshots")

    scan = bronze_table.scan()
    if since is not None:
        scan = bronze_table.scan(row_filter=f"_ingested_at > '{since.isoformat()}'")

    dim_symbols = _load_dim_symbol(catalog)
    now = datetime.now(timezone.utc)

    total_written = 0
    reader = scan.to_arrow_batch_reader()

    accumulated = []
    accumulated_rows = 0

    for record_batch in reader:
        if record_batch.num_rows == 0:
            continue

        accumulated.append(record_batch)
        accumulated_rows += record_batch.num_rows

        if accumulated_rows >= TRADE_BATCH_SIZE:
            batch_table = pa.Table.from_batches(accumulated, schema=reader.schema)
            accumulated = []
            accumulated_rows = 0

            silver_batch = _process_orderbook_batch(batch_table, dim_symbols, now)
            if silver_batch is not None and len(silver_batch) > 0:
                silver_table.append(silver_batch)
                total_written += len(silver_batch)
                logger.info("Flushed %d Silver orderbook records (total: %d).", len(silver_batch), total_written)

    # Flush remaining
    if accumulated:
        batch_table = pa.Table.from_batches(accumulated, schema=reader.schema)
        silver_batch = _process_orderbook_batch(batch_table, dim_symbols, now)
        if silver_batch is not None and len(silver_batch) > 0:
            silver_table.append(silver_batch)
            total_written += len(silver_batch)
            logger.info("Flushed %d Silver orderbook records (total: %d).", len(silver_batch), total_written)

    if total_written == 0:
        logger.info("No new Bronze orderbook records to process.")
    else:
        logger.info("Wrote %d total records to silver.orderbook_snapshots.", total_written)
    return total_written


def run_silver(since: datetime | None = None) -> None:
    """Run the full Bronze → Silver pipeline."""
    catalog = get_catalog()
    process_trades(catalog, since)
    process_orderbook(catalog, since)
    logger.info("Silver processing complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_silver()
