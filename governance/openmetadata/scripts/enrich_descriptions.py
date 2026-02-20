"""enrich_descriptions.py
Populate table and column descriptions in OpenMetadata via the REST API.
Run after bootstrap.sh once the Iceberg metadata has been ingested.

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python enrich_descriptions.py
"""

import os
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585")
OM_TOKEN = os.environ.get("OM_TOKEN", "CHANGE_ME")
API = f"{OM_HOST}/api/v1"
HEADERS = {
    "Authorization": f"Bearer {OM_TOKEN}",
    "Content-Type": "application/json",
}

# ─── Table descriptions ───────────────────────────────────────────────────────

TABLE_DESCRIPTIONS: dict[str, str] = {
    "de-capstone-iceberg.iceberg.bronze.raw_trades": (
        "Raw trade events streamed from the Kafka `trades` topic via Flink. "
        "One row per trade tick; no deduplication or enrichment applied."
    ),
    "de-capstone-iceberg.iceberg.bronze.raw_order_book": (
        "Raw order-book snapshots from the Kafka `order_book` topic. "
        "Captures bid/ask ladder at each event timestamp."
    ),
    "de-capstone-iceberg.iceberg.silver.trades": (
        "Cleansed trade data with DECIMAL(18,6) prices, deduplication by "
        "trade_id, and partition by trade_date."
    ),
    "de-capstone-iceberg.iceberg.silver.ohlcv": (
        "One-minute OHLCV bars derived from silver.trades. "
        "Includes volume-weighted average price (VWAP)."
    ),
    "de-capstone-iceberg.iceberg.gold.daily_trading_summary": (
        "Daily aggregated trading statistics per symbol: total volume, "
        "VWAP, high/low, trade count, and bid-ask spread metrics."
    ),
    "de-capstone-iceberg.iceberg.gold.top_movers": (
        "Daily top-N symbols ranked by absolute price change percentage."
    ),
}

# ─── Column descriptions ──────────────────────────────────────────────────────

COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "de-capstone-iceberg.iceberg.bronze.raw_trades": {
        "trade_id": "Unique identifier for the trade, assigned by the exchange simulator.",
        "symbol": "Ticker symbol (e.g. AAPL, MSFT).",
        "price": "Execution price as a string; cast to DECIMAL(18,6) in silver layer.",
        "quantity": "Number of shares traded.",
        "side": "Trade direction: BUY or SELL.",
        "timestamp": "Event timestamp (epoch milliseconds) from the Kafka message.",
        "account_id": "PII: originating account identifier — masked in analyst views.",
    },
    "de-capstone-iceberg.iceberg.silver.trades": {
        "trade_id": "Unique trade identifier (deduplicated from bronze).",
        "symbol": "Ticker symbol.",
        "price": "Execution price as DECIMAL(18,6).",
        "quantity": "Number of shares traded.",
        "side": "Trade direction: BUY or SELL.",
        "trade_date": "Partition column: date portion of the trade timestamp.",
        "account_id": "PII: pseudonymised account identifier (SHA-256 hash).",
    },
    "de-capstone-iceberg.iceberg.gold.daily_trading_summary": {
        "symbol": "Ticker symbol.",
        "trade_date": "Partition column: trading date.",
        "total_volume": "Total shares traded during the session.",
        "vwap": "Volume-weighted average price for the session.",
        "open_price": "First executed price of the session.",
        "close_price": "Last executed price of the session.",
        "high_price": "Highest executed price of the session.",
        "low_price": "Lowest executed price of the session.",
        "trade_count": "Total number of trades executed.",
        "avg_bid_ask_spread": "Average bid-ask spread across all order-book snapshots.",
    },
}


def _get_table_id(fqn: str) -> str | None:
    resp = requests.get(
        f"{API}/tables/name/{fqn}",
        headers=HEADERS,
        params={"fields": "id"},
    )
    if resp.status_code == 200:
        return resp.json().get("id")
    log.warning("Table not found: %s (HTTP %d)", fqn, resp.status_code)
    return None


def patch_table_description(fqn: str, description: str) -> None:
    table_id = _get_table_id(fqn)
    if not table_id:
        return
    patch = [{"op": "add", "path": "/description", "value": description}]
    resp = requests.patch(
        f"{API}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch,
    )
    if resp.status_code in (200, 204):
        log.info("Updated description for table %s", fqn)
    else:
        log.error("Failed to update %s: %d %s", fqn, resp.status_code, resp.text[:200])


def patch_column_description(fqn: str, column: str, description: str) -> None:
    table_id = _get_table_id(fqn)
    if not table_id:
        return
    # Fetch the full column list first to find the column index
    resp = requests.get(f"{API}/tables/{table_id}", headers=HEADERS, params={"fields": "columns"})
    if resp.status_code != 200:
        return
    columns = resp.json().get("columns", [])
    idx = next((i for i, c in enumerate(columns) if c["name"] == column), None)
    if idx is None:
        log.warning("Column %s not found in table %s", column, fqn)
        return
    patch = [{"op": "add", "path": f"/columns/{idx}/description", "value": description}]
    resp = requests.patch(
        f"{API}/tables/{table_id}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch,
    )
    if resp.status_code in (200, 204):
        log.info("Updated column %s.%s", fqn, column)
    else:
        log.error("Failed column %s.%s: %d", fqn, column, resp.status_code)


def main() -> None:
    log.info("Enriching table descriptions (%d tables)...", len(TABLE_DESCRIPTIONS))
    for fqn, desc in TABLE_DESCRIPTIONS.items():
        patch_table_description(fqn, desc)

    log.info("Enriching column descriptions...")
    for fqn, columns in COLUMN_DESCRIPTIONS.items():
        for col, desc in columns.items():
            patch_column_description(fqn, col, desc)

    log.info("Done.")


if __name__ == "__main__":
    main()
