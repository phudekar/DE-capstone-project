"""SCD Type 2 implementation for dim_symbol using hash-based change detection."""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone

import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)

TRACKED_COLUMNS = ("company_name", "sector", "market_cap_category")


def _hash_row(symbol: str, company_name: str, sector: str, market_cap: str) -> str:
    """Deterministic hash for change detection on tracked columns."""
    payload = f"{symbol}|{company_name}|{sector}|{market_cap}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def apply_scd2(incoming_records: list[dict]) -> tuple[int, int]:
    """Apply SCD Type 2 to dim_symbol.

    For each incoming record, compare its hash against the current row.
    If changed: expire the old row, insert new version.
    If new: insert directly.

    Args:
        incoming_records: list of dicts with keys:
            symbol, company_name, sector, market_cap_category

    Returns:
        (expired_count, inserted_count)
    """
    catalog = get_catalog()
    table = catalog.load_table(f"{config.NS_DIM}.dim_symbol")

    # Load current records into a lookup
    scan = table.scan(row_filter="is_current == true")
    current_arrow = scan.to_arrow()

    current_by_symbol: dict[str, dict] = {}
    if len(current_arrow) > 0:
        for i in range(len(current_arrow)):
            row = {col: current_arrow.column(col)[i].as_py() for col in current_arrow.column_names}
            current_by_symbol[row["symbol"]] = row

    # Determine next surrogate key
    max_key = max((r["symbol_key"] for r in current_by_symbol.values()), default=0)

    today = date.today()
    yesterday = date.fromordinal(today.toordinal() - 1)
    now = datetime.now(timezone.utc)
    far_future = date(9999, 12, 31)

    expired_rows = []
    new_rows = []

    for rec in incoming_records:
        new_hash = _hash_row(
            rec["symbol"], rec["company_name"], rec["sector"], rec["market_cap_category"]
        )

        if rec["symbol"] in current_by_symbol:
            existing = current_by_symbol[rec["symbol"]]
            if existing["row_hash"] == new_hash:
                continue  # No change

            # Expire old row — write the expired version
            expired_rows.append(
                {
                    **existing,
                    "expiry_date": yesterday,
                    "is_current": False,
                    "_updated_at": now,
                }
            )
            max_key += 1
            new_rows.append(
                {
                    "symbol_key": max_key,
                    "symbol": rec["symbol"],
                    "company_name": rec["company_name"],
                    "sector": rec["sector"],
                    "market_cap_category": rec["market_cap_category"],
                    "effective_date": today,
                    "expiry_date": far_future,
                    "is_current": True,
                    "row_hash": new_hash,
                    "_updated_at": now,
                }
            )
        else:
            # Brand new symbol
            max_key += 1
            new_rows.append(
                {
                    "symbol_key": max_key,
                    "symbol": rec["symbol"],
                    "company_name": rec["company_name"],
                    "sector": rec["sector"],
                    "market_cap_category": rec["market_cap_category"],
                    "effective_date": today,
                    "expiry_date": far_future,
                    "is_current": True,
                    "row_hash": new_hash,
                    "_updated_at": now,
                }
            )

    expired_count = 0
    inserted_count = 0

    if expired_rows:
        # Overwrite expired rows using delete + append
        # Delete old current=True rows for changed symbols
        changed_symbols = {r["symbol"] for r in expired_rows}
        delete_filter = " or ".join(
            f"(symbol == '{s}' and is_current == true)" for s in changed_symbols
        )
        table.delete(delete_filter)
        # Re-insert the expired versions
        expired_table = pa.table(
            {k: [r[k] for r in expired_rows] for k in expired_rows[0]}
        )
        table.append(expired_table)
        expired_count = len(expired_rows)
        logger.info("Expired %d rows in dim_symbol.", expired_count)

    if new_rows:
        new_table = pa.table({k: [r[k] for r in new_rows] for k in new_rows[0]})
        table.append(new_table)
        inserted_count = len(new_rows)
        logger.info("Inserted %d new rows in dim_symbol.", inserted_count)

    return expired_count, inserted_count
