"""Populate dim_symbol and dim_time tables with initial data."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime, timezone

import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)


def _hash_symbol_row(symbol: str, company_name: str, sector: str, market_cap: str) -> str:
    """Deterministic hash for SCD2 change detection."""
    payload = f"{symbol}|{company_name}|{sector}|{market_cap}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def seed_dim_symbol(catalog) -> int:
    """Load symbols from reference JSON into dim_symbol (SCD2 initial load)."""
    with open(config.REFERENCE_DATA_PATH) as f:
        symbols = json.load(f)

    today = date.today()
    now = datetime.now(timezone.utc)
    far_future = date(9999, 12, 31)

    rows = {
        "symbol_key": [],
        "symbol": [],
        "company_name": [],
        "sector": [],
        "market_cap_category": [],
        "effective_date": [],
        "expiry_date": [],
        "is_current": [],
        "row_hash": [],
        "_updated_at": [],
    }

    for i, s in enumerate(symbols, start=1):
        rows["symbol_key"].append(i)
        rows["symbol"].append(s["symbol"])
        rows["company_name"].append(s["company_name"])
        rows["sector"].append(s["sector"])
        rows["market_cap_category"].append(s["market_cap_category"])
        rows["effective_date"].append(today)
        rows["expiry_date"].append(far_future)
        rows["is_current"].append(True)
        rows["row_hash"].append(_hash_symbol_row(s["symbol"], s["company_name"], s["sector"], s["market_cap_category"]))
        rows["_updated_at"].append(now)

    arrow_schema = pa.schema(
        [
            pa.field("symbol_key", pa.int32(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("company_name", pa.string(), nullable=False),
            pa.field("sector", pa.string(), nullable=False),
            pa.field("market_cap_category", pa.string(), nullable=False),
            pa.field("effective_date", pa.date32(), nullable=False),
            pa.field("expiry_date", pa.date32(), nullable=False),
            pa.field("is_current", pa.bool_(), nullable=False),
            pa.field("row_hash", pa.string(), nullable=False),
            pa.field("_updated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )
    arrow_table = pa.table(rows, schema=arrow_schema)
    table = catalog.load_table(f"{config.NS_DIM}.dim_symbol")
    table.append(arrow_table)
    logger.info("Seeded dim_symbol with %d symbols.", len(symbols))
    return len(symbols)


def seed_dim_time(catalog) -> int:
    """Pre-populate dim_time with one row per minute (24h = 1440 rows)."""
    rows = {
        "time_key": [],
        "hour": [],
        "minute": [],
        "time_of_day": [],
        "trading_session": [],
        "is_market_hours": [],
    }

    for h in range(24):
        for m in range(60):
            key = h * 100 + m  # e.g. 930 for 09:30
            rows["time_key"].append(key)
            rows["hour"].append(h)
            rows["minute"].append(m)
            rows["time_of_day"].append(f"{h:02d}:{m:02d}")

            # Trading sessions (US market hours)
            if 4 <= h < 9 or (h == 9 and m < 30):
                session = "pre-market"
            elif (h == 9 and m >= 30) or (10 <= h < 16):
                session = "regular"
            elif 16 <= h < 20:
                session = "after-hours"
            else:
                session = "closed"

            rows["trading_session"].append(session)
            rows["is_market_hours"].append(session == "regular")

    arrow_schema = pa.schema(
        [
            pa.field("time_key", pa.int32(), nullable=False),
            pa.field("hour", pa.int32(), nullable=False),
            pa.field("minute", pa.int32(), nullable=False),
            pa.field("time_of_day", pa.string(), nullable=False),
            pa.field("trading_session", pa.string(), nullable=False),
            pa.field("is_market_hours", pa.bool_(), nullable=False),
        ]
    )
    arrow_table = pa.table(rows, schema=arrow_schema)
    table = catalog.load_table(f"{config.NS_DIM}.dim_time")
    table.append(arrow_table)
    logger.info("Seeded dim_time with 1440 minute rows.")
    return 1440


def seed_all():
    """Run all dimension seeding."""
    catalog = get_catalog()
    seed_dim_symbol(catalog)
    seed_dim_time(catalog)
    logger.info("Dimension seeding complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    seed_all()
