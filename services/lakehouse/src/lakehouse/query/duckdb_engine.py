"""DuckDB query engine over Iceberg tables via PyIceberg → Arrow → DuckDB."""

from __future__ import annotations

import logging
from datetime import date

import duckdb
import pyarrow as pa

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """Query helper that reads Iceberg tables via PyIceberg and queries with DuckDB."""

    def __init__(self, catalog=None):
        self._catalog = catalog or get_catalog()

    def _scan_to_arrow(self, table_name: str, row_filter: str | None = None) -> pa.Table:
        """Load an Iceberg table into an Arrow table."""
        table = self._catalog.load_table(table_name)
        scan = table.scan(row_filter=row_filter) if row_filter else table.scan()
        return scan.to_arrow()

    def trades_by_symbol(self, symbol: str, limit: int = 100) -> pa.Table:
        """Get recent trades for a symbol from Silver."""
        arrow = self._scan_to_arrow(
            f"{config.NS_SILVER}.trades", row_filter=f"symbol == '{symbol}'"
        )
        con = duckdb.connect()
        con.register("trades", arrow)
        result = con.execute(
            "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?", [limit]
        ).fetch_arrow_table()
        con.close()
        return result

    def daily_summary(self, trading_date: date | None = None) -> pa.Table:
        """Get daily trading summary from Gold."""
        if trading_date is None:
            trading_date = date.today()
        return self._scan_to_arrow(
            f"{config.NS_GOLD}.daily_trading_summary",
            row_filter=f"trading_date == '{trading_date.isoformat()}'",
        )

    def top_movers(self, trading_date: date | None = None, limit: int = 5) -> pa.Table:
        """Get top movers by price range (high - low) from Gold."""
        if trading_date is None:
            trading_date = date.today()
        arrow = self._scan_to_arrow(
            f"{config.NS_GOLD}.daily_trading_summary",
            row_filter=f"trading_date == '{trading_date.isoformat()}'",
        )
        if len(arrow) == 0:
            return arrow
        con = duckdb.connect()
        con.register("summary", arrow)
        result = con.execute(
            """
            SELECT symbol, company_name, sector,
                   open_price, close_price, high_price, low_price,
                   (close_price - open_price) / open_price * 100 AS pct_change,
                   total_volume, trade_count
            FROM summary
            ORDER BY abs(pct_change) DESC
            LIMIT ?
            """,
            [limit],
        ).fetch_arrow_table()
        con.close()
        return result

    def orderbook_spread(self, symbol: str, limit: int = 50) -> pa.Table:
        """Get recent orderbook spreads for a symbol from Silver."""
        arrow = self._scan_to_arrow(
            f"{config.NS_SILVER}.orderbook_snapshots",
            row_filter=f"symbol == '{symbol}'",
        )
        if len(arrow) == 0:
            return arrow
        con = duckdb.connect()
        con.register("ob", arrow)
        result = con.execute(
            """
            SELECT symbol, timestamp, best_bid_price, best_ask_price,
                   spread, mid_price, bid_depth, ask_depth
            FROM ob
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [limit],
        ).fetch_arrow_table()
        con.close()
        return result
