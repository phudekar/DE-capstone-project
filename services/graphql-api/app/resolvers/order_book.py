"""Order book resolver — queries the Silver layer orderbook_snapshots table."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from app.db.iceberg_duckdb import IcebergDuckDB
from app.schema.types import OrderBookSnapshot


class OrderBookResolver:
    def __init__(self, engine: IcebergDuckDB) -> None:
        self.engine = engine

    async def resolve(self, symbol: str) -> Optional[OrderBookSnapshot]:
        rows = await self.engine.execute(
            namespace="silver",
            table="orderbook_snapshots",
            sql="""
                SELECT symbol, timestamp, best_bid_price, best_bid_qty,
                       best_ask_price, best_ask_qty, bid_depth, ask_depth,
                       spread, mid_price, company_name, sector
                FROM t
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """,
            params=[symbol],
        )
        if not rows:
            return None
        r = rows[0]
        ts = r["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        return OrderBookSnapshot(
            symbol=r["symbol"],
            timestamp=ts,
            best_bid_price=r.get("best_bid_price"),
            best_bid_qty=r.get("best_bid_qty"),
            best_ask_price=r.get("best_ask_price"),
            best_ask_qty=r.get("best_ask_qty"),
            bid_depth=int(r.get("bid_depth", 0)),
            ask_depth=int(r.get("ask_depth", 0)),
            spread=r.get("spread"),
            mid_price=r.get("mid_price"),
            company_name=r.get("company_name"),
            sector=r.get("sector"),
        )
