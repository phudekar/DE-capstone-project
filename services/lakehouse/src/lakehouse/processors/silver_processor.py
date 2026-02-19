"""Silver processor: Bronze → Silver (deduplicate, cast, enrich)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)


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


def process_trades(catalog, since: datetime | None = None) -> int:
    """Read unprocessed Bronze trades, deduplicate, enrich, write to Silver.

    Args:
        catalog: PyIceberg catalog
        since: only process records ingested after this timestamp.
               If None, processes all Bronze records.

    Returns:
        Number of Silver records written.
    """
    bronze_table = catalog.load_table(f"{config.NS_BRONZE}.raw_trades")
    silver_table = catalog.load_table(f"{config.NS_SILVER}.trades")

    # Scan Bronze
    scan = bronze_table.scan()
    if since is not None:
        scan = bronze_table.scan(row_filter=f"_ingested_at > '{since.isoformat()}'")
    bronze_arrow = scan.to_arrow()

    if len(bronze_arrow) == 0:
        logger.info("No new Bronze trades to process.")
        return 0

    # Deduplicate by trade_id (keep first occurrence by _kafka_offset)
    df = bronze_arrow.to_pandas()
    df = df.sort_values("_kafka_offset").drop_duplicates(subset=["trade_id"], keep="first")

    # Also deduplicate against existing Silver
    existing_scan = silver_table.scan(selected_fields=("trade_id",))
    existing_arrow = existing_scan.to_arrow()
    if len(existing_arrow) > 0:
        existing_ids = set(existing_arrow.column("trade_id").to_pylist())
        df = df[~df["trade_id"].isin(existing_ids)]

    if len(df) == 0:
        logger.info("All Bronze trades already in Silver.")
        return 0

    # Enrich with dim_symbol
    dim_symbols = _load_dim_symbol(catalog)
    now = datetime.now(timezone.utc)

    silver_rows = {
        "trade_id": [],
        "symbol": [],
        "price": [],
        "quantity": [],
        "buyer_order_id": [],
        "seller_order_id": [],
        "buyer_agent_id": [],
        "seller_agent_id": [],
        "aggressor_side": [],
        "timestamp": [],
        "company_name": [],
        "sector": [],
        "_processed_at": [],
    }

    for _, row in df.iterrows():
        sym = row["symbol"]
        dim = dim_symbols.get(sym, {})
        silver_rows["trade_id"].append(row["trade_id"])
        silver_rows["symbol"].append(sym)
        silver_rows["price"].append(float(row["price"]))
        silver_rows["quantity"].append(int(row["quantity"]))
        silver_rows["buyer_order_id"].append(row["buyer_order_id"])
        silver_rows["seller_order_id"].append(row["seller_order_id"])
        silver_rows["buyer_agent_id"].append(row["buyer_agent_id"])
        silver_rows["seller_agent_id"].append(row["seller_agent_id"])
        silver_rows["aggressor_side"].append(row["aggressor_side"])
        silver_rows["timestamp"].append(row["timestamp"])
        silver_rows["company_name"].append(dim.get("company_name"))
        silver_rows["sector"].append(dim.get("sector"))
        silver_rows["_processed_at"].append(now)

    silver_arrow = pa.table(silver_rows)
    silver_table.append(silver_arrow)
    count = len(silver_arrow)
    logger.info("Wrote %d records to silver.trades.", count)
    return count


def process_orderbook(catalog, since: datetime | None = None) -> int:
    """Read unprocessed Bronze orderbook, extract top-of-book, enrich, write Silver.

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
    bronze_arrow = scan.to_arrow()

    if len(bronze_arrow) == 0:
        logger.info("No new Bronze orderbook records to process.")
        return 0

    # Deduplicate by (symbol, timestamp)
    df = bronze_arrow.to_pandas()
    df = df.sort_values("_kafka_offset").drop_duplicates(
        subset=["symbol", "timestamp"], keep="first"
    )

    dim_symbols = _load_dim_symbol(catalog)
    now = datetime.now(timezone.utc)

    silver_rows = {
        "symbol": [],
        "timestamp": [],
        "best_bid_price": [],
        "best_bid_qty": [],
        "best_ask_price": [],
        "best_ask_qty": [],
        "bid_depth": [],
        "ask_depth": [],
        "spread": [],
        "mid_price": [],
        "sequence_number": [],
        "company_name": [],
        "sector": [],
        "_processed_at": [],
    }

    for _, row in df.iterrows():
        bids = json.loads(row["bids_json"]) if row["bids_json"] else []
        asks = json.loads(row["asks_json"]) if row["asks_json"] else []
        sym = row["symbol"]
        dim = dim_symbols.get(sym, {})

        best_bid = bids[0] if bids else None
        best_ask = asks[0] if asks else None
        best_bid_price = best_bid["price"] if best_bid else None
        best_ask_price = best_ask["price"] if best_ask else None

        spread = None
        mid_price = None
        if best_bid_price is not None and best_ask_price is not None:
            spread = best_ask_price - best_bid_price
            mid_price = (best_bid_price + best_ask_price) / 2.0

        silver_rows["symbol"].append(sym)
        silver_rows["timestamp"].append(row["timestamp"])
        silver_rows["best_bid_price"].append(best_bid_price)
        silver_rows["best_bid_qty"].append(best_bid["quantity"] if best_bid else None)
        silver_rows["best_ask_price"].append(best_ask_price)
        silver_rows["best_ask_qty"].append(best_ask["quantity"] if best_ask else None)
        silver_rows["bid_depth"].append(len(bids))
        silver_rows["ask_depth"].append(len(asks))
        silver_rows["spread"].append(spread)
        silver_rows["mid_price"].append(mid_price)
        silver_rows["sequence_number"].append(int(row["sequence_number"]))
        silver_rows["company_name"].append(dim.get("company_name"))
        silver_rows["sector"].append(dim.get("sector"))
        silver_rows["_processed_at"].append(now)

    silver_arrow = pa.table(silver_rows)
    silver_table.append(silver_arrow)
    count = len(silver_arrow)
    logger.info("Wrote %d records to silver.orderbook_snapshots.", count)
    return count


def run_silver(since: datetime | None = None) -> None:
    """Run the full Bronze → Silver pipeline."""
    catalog = get_catalog()
    process_trades(catalog, since)
    process_orderbook(catalog, since)
    logger.info("Silver processing complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_silver()
