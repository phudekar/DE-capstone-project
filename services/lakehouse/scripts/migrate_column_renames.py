#!/usr/bin/env python3
"""Migrate Iceberg tables after DE-Stock column renames (commit f148d58).

Renames applied:
  bronze.raw_trades:
    buyer_order_id  → buy_order_id
    seller_order_id → sell_order_id
    aggressor_side  → is_aggressive_buy  (also String → Boolean type change)

  silver.trades:
    buyer_order_id  → buy_order_id
    seller_order_id → sell_order_id
    aggressor_side  → is_aggressive_buy

Iceberg schema evolution is metadata-only: no data files are rewritten.
For the type change (String → Boolean) on aggressor_side/is_aggressive_buy,
Iceberg cannot do an in-place type change. We drop the old column and add
the new one. Existing rows will have NULL for is_aggressive_buy until
reprocessed.

Usage:
    python -m scripts.migrate_column_renames          # dry-run (default)
    python -m scripts.migrate_column_renames --apply  # apply changes
"""

import argparse
import logging
import sys

from pyiceberg.types import BooleanType

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)

# (table_identifier, old_name, new_name, type_change_to)
# type_change_to is None for pure renames, or a PyIceberg type for drop+add
RENAMES = [
    # Bronze raw_trades
    (f"{config.NS_BRONZE}.raw_trades", "buyer_order_id", "buy_order_id", None),
    (f"{config.NS_BRONZE}.raw_trades", "seller_order_id", "sell_order_id", None),
    (f"{config.NS_BRONZE}.raw_trades", "aggressor_side", "is_aggressive_buy", BooleanType()),
    # Silver trades
    (f"{config.NS_SILVER}.trades", "buyer_order_id", "buy_order_id", None),
    (f"{config.NS_SILVER}.trades", "seller_order_id", "sell_order_id", None),
    (f"{config.NS_SILVER}.trades", "aggressor_side", "is_aggressive_buy", None),
]


def _column_exists(table, name: str) -> bool:
    """Check whether a column exists in the current table schema."""
    try:
        table.schema().find_field(name)
        return True
    except ValueError:
        return False


def migrate(dry_run: bool = True) -> None:
    catalog = get_catalog()

    for table_id, old_name, new_name, type_change in RENAMES:
        try:
            table = catalog.load_table(table_id)
        except Exception:
            logger.warning("Table %s not found — skipping.", table_id)
            continue

        # Already migrated?
        if _column_exists(table, new_name):
            logger.info("SKIP  %s.%s — already exists.", table_id, new_name)
            continue

        if not _column_exists(table, old_name):
            logger.warning(
                "SKIP  %s — neither %s nor %s found.", table_id, old_name, new_name
            )
            continue

        if type_change is not None:
            # Type changed: must drop old column and add new one
            action = f"DROP {old_name} + ADD {new_name} ({type_change})"
            if dry_run:
                logger.info("DRY-RUN  %s: %s", table_id, action)
            else:
                with table.update_schema() as update:
                    update.delete_column(old_name)
                    update.add_column(new_name, type_change)
                logger.info("APPLIED  %s: %s", table_id, action)
        else:
            # Pure rename — metadata only
            action = f"RENAME {old_name} → {new_name}"
            if dry_run:
                logger.info("DRY-RUN  %s: %s", table_id, action)
            else:
                with table.update_schema() as update:
                    update.rename_column(old_name, new_name)
                logger.info("APPLIED  %s: %s", table_id, action)

    mode = "DRY-RUN" if dry_run else "APPLIED"
    logger.info("Migration complete (%s).", mode)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag, runs in dry-run mode.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    migrate(dry_run=not args.apply)


if __name__ == "__main__":
    main()
