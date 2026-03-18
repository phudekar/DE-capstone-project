"""Gold aggregator: Silver → Gold layer using DuckDB for daily OHLCV aggregation.

This processor reads deduplicated, enriched trade records from the Silver layer
(silver.trades) and produces daily trading summaries in the Gold layer
(gold.daily_trading_summary). Each Gold record contains OHLCV (open, high, low,
close, volume) data plus VWAP (volume-weighted average price) per symbol per day.

Upstream: silver.trades (produced by silver_processor)
Downstream: gold.daily_trading_summary (consumed by GraphQL API, Superset dashboards)

Key design decisions:
- Uses DuckDB for in-process SQL aggregation (no external compute cluster needed).
- Idempotent: deletes existing Gold records for the target date before writing,
  so re-runs produce the same result.
- VWAP is calculated as sum(price * quantity) / sum(quantity), guarded against
  divide-by-zero.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import duckdb
import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)

GOLD_DAILY_SUMMARY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("trading_date", pa.date32(), nullable=False),
        pa.field("open_price", pa.float64(), nullable=False),
        pa.field("close_price", pa.float64(), nullable=False),
        pa.field("high_price", pa.float64(), nullable=False),
        pa.field("low_price", pa.float64(), nullable=False),
        pa.field("vwap", pa.float64(), nullable=False),
        pa.field("total_volume", pa.int64(), nullable=False),
        pa.field("trade_count", pa.int32(), nullable=False),
        pa.field("total_value", pa.float64(), nullable=False),
        pa.field("company_name", pa.string(), nullable=True),
        pa.field("sector", pa.string(), nullable=True),
        pa.field("_aggregated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def aggregate_daily_trading_summary(catalog, trading_date: date | None = None) -> int:
    """Compute daily trading summary from Silver trades using DuckDB.

    Args:
        catalog: PyIceberg catalog
        trading_date: date to aggregate. Defaults to today.

    Returns:
        Number of Gold records written.
    """
    if trading_date is None:
        trading_date = date.today()

    silver_table = catalog.load_table(f"{config.NS_SILVER}.trades")
    gold_table = catalog.load_table(f"{config.NS_GOLD}.daily_trading_summary")

    # Read Silver trades into Arrow — filter to target date to limit memory.
    # Use start-of-next-day as the upper bound instead of 23:59:59, so that
    # trades occurring during the last second of the day (23:59:59.000–23:59:59.999)
    # are not excluded from the aggregation.
    ds = trading_date.isoformat()
    next_day = (trading_date + timedelta(days=1)).isoformat()
    date_filter = f"timestamp >= '{ds}T00:00:00+00:00' AND timestamp < '{next_day}T00:00:00+00:00'"
    silver_arrow = silver_table.scan(row_filter=date_filter).to_arrow()

    if len(silver_arrow) == 0:
        logger.info("No Silver trades to aggregate.")
        return 0

    # Use DuckDB to aggregate
    con = duckdb.connect()
    # Phase 12: performance settings
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '512MB'")
    con.execute("SET enable_object_cache = true")
    con.register("silver_trades", silver_arrow)

    result = con.execute(
        """
        SELECT
            symbol,
            CAST(? AS DATE) AS trading_date,
            first(price ORDER BY timestamp ASC) AS open_price,
            last(price ORDER BY timestamp ASC) AS close_price,
            max(price) AS high_price,
            min(price) AS low_price,
            CASE WHEN sum(quantity) > 0
                THEN sum(price * quantity) / sum(quantity)
                ELSE 0.0
            END AS vwap,
            CAST(sum(quantity) AS BIGINT) AS total_volume,
            CAST(count(*) AS INTEGER) AS trade_count,
            sum(price * quantity) AS total_value,
            first(company_name) AS company_name,
            first(sector) AS sector,
        FROM silver_trades
        WHERE CAST(timestamp AS DATE) = ?
        GROUP BY symbol
        HAVING count(*) > 0
        ORDER BY symbol
        """,
        [trading_date, trading_date],
    ).fetch_arrow_table()
    con.close()

    if len(result) == 0:
        logger.info("No trades found for date %s.", trading_date)
        return 0

    # Add aggregation timestamp
    now = datetime.now(timezone.utc)
    agg_at = pa.array([now] * len(result), type=pa.timestamp("us", tz="UTC"))
    result = result.append_column("_aggregated_at", agg_at)

    # Cast to explicit schema with correct nullability
    result = result.cast(GOLD_DAILY_SUMMARY_ARROW_SCHEMA)

    # Delete existing Gold records for this date (idempotent overwrite)
    gold_table.delete(f"trading_date == '{trading_date.isoformat()}'")
    gold_table.append(result)

    count = len(result)
    logger.info("Wrote %d daily summaries for %s to gold.daily_trading_summary.", count, trading_date)
    return count


def run_gold(trading_date: date | None = None) -> None:
    """Run the full Silver → Gold pipeline."""
    catalog = get_catalog()
    aggregate_daily_trading_summary(catalog, trading_date)
    logger.info("Gold aggregation complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_gold()
