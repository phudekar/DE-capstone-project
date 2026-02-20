"""Trade resolver — queries the Silver layer via DuckDB/Iceberg."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.db.iceberg_duckdb import IcebergDuckDB
from app.schema.inputs import TradeFilterInput
from app.schema.pagination import (
    TradeConnection,
    TradeEdge,
    PageInfo,
    encode_cursor,
    decode_cursor,
)
from app.schema.types import Trade


def _row_to_trade(row: dict) -> Trade:
    ts = row["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return Trade(
        trade_id=str(row["trade_id"]),
        symbol=row["symbol"],
        price=float(row["price"]),
        quantity=int(row["quantity"]),
        buyer_agent_id=row.get("buyer_agent_id", ""),
        seller_agent_id=row.get("seller_agent_id", ""),
        aggressor_side=row.get("aggressor_side", ""),
        timestamp=ts,
        company_name=row.get("company_name"),
        sector=row.get("sector"),
    )


def _build_where(f: TradeFilterInput) -> tuple[str, list]:
    """Build dynamic WHERE clauses from filter input."""
    clauses: list[str] = ["1=1"]
    params: list = []

    if f.symbol:
        clauses.append("symbol = ?")
        params.append(f.symbol)
    if f.symbols:
        placeholders = ",".join(["?" for _ in f.symbols])
        clauses.append(f"symbol IN ({placeholders})")
        params.extend(f.symbols)
    if f.aggressor_side:
        clauses.append("aggressor_side = ?")
        params.append(f.aggressor_side)
    if f.min_price is not None:
        clauses.append("price >= ?")
        params.append(f.min_price)
    if f.max_price is not None:
        clauses.append("price <= ?")
        params.append(f.max_price)
    if f.min_quantity is not None:
        clauses.append("quantity >= ?")
        params.append(f.min_quantity)
    if f.date_range:
        clauses.append("CAST(timestamp AS DATE) BETWEEN ? AND ?")
        params.extend([str(f.date_range.start), str(f.date_range.end)])

    return " AND ".join(clauses), params


class TradeResolver:
    def __init__(self, engine: IcebergDuckDB) -> None:
        self.engine = engine

    async def resolve(
        self,
        filter: Optional[TradeFilterInput] = None,
        first: int = 20,
        after: Optional[str] = None,
    ) -> TradeConnection:
        offset = decode_cursor(after) if after else 0
        where_clause, params = _build_where(filter or TradeFilterInput())

        # Fetch first+1 rows to determine has_next_page
        rows = await self.engine.execute(
            namespace="silver",
            table="trades",
            sql=f"""
                SELECT trade_id, symbol, price, quantity,
                       buyer_agent_id, seller_agent_id, aggressor_side,
                       timestamp, company_name, sector
                FROM t
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            """,
            params=params + [first + 1, offset],
        )

        has_next = len(rows) > first
        page_rows = rows[:first]
        edges = [
            TradeEdge(node=_row_to_trade(r), cursor=encode_cursor(offset + i))
            for i, r in enumerate(page_rows)
        ]

        return TradeConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next,
                has_previous_page=offset > 0,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=await self._count(where_clause, params),
        )

    async def resolve_by_id(self, trade_id: str) -> Optional[Trade]:
        rows = await self.engine.execute(
            namespace="silver",
            table="trades",
            sql="SELECT * FROM t WHERE trade_id = ? LIMIT 1",
            params=[trade_id],
        )
        if not rows:
            return None
        return _row_to_trade(rows[0])

    async def _count(self, where_clause: str, params: list) -> int:
        rows = await self.engine.execute(
            namespace="silver",
            table="trades",
            sql=f"SELECT COUNT(*) AS n FROM t WHERE {where_clause}",
            params=params,
        )
        return rows[0]["n"] if rows else 0
