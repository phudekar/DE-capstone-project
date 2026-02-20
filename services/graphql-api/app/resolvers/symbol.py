"""Symbol resolver — queries the dim_symbol dimension table."""

from __future__ import annotations

from typing import Optional

from app.cache.memory_cache import MemoryCache
from app.config import settings
from app.db.iceberg_duckdb import IcebergDuckDB
from app.schema.inputs import SymbolFilterInput
from app.schema.pagination import (
    SymbolConnection,
    SymbolEdge,
    PageInfo,
    encode_cursor,
    decode_cursor,
)
from app.schema.types import Symbol


def _row_to_symbol(row: dict) -> Symbol:
    return Symbol(
        symbol=row["symbol"],
        company_name=row["company_name"],
        sector=row["sector"],
        market_cap_category=row["market_cap_category"],
        is_current=bool(row.get("is_current", True)),
    )


class SymbolResolver:
    def __init__(self, engine: IcebergDuckDB, cache: MemoryCache) -> None:
        self.engine = engine
        self.cache = cache

    async def resolve(
        self,
        filter: Optional[SymbolFilterInput] = None,
        first: int = 50,
        after: Optional[str] = None,
    ) -> SymbolConnection:
        offset = decode_cursor(after) if after else 0
        f = filter or SymbolFilterInput()

        clauses = ["is_current = true"]
        params: list = []

        if f.is_current is not None:
            clauses[0] = f"is_current = {str(f.is_current).lower()}"
        if f.sector:
            clauses.append("sector = ?")
            params.append(f.sector)
        if f.market_cap_category:
            clauses.append("market_cap_category = ?")
            params.append(f.market_cap_category)
        if f.search:
            clauses.append("(symbol ILIKE ? OR company_name ILIKE ?)")
            pattern = f"%{f.search}%"
            params.extend([pattern, pattern])

        where = " AND ".join(clauses)
        cache_ns = "symbols"
        cache_key = {"where": where, "params": params}

        cached = await self.cache.get(cache_ns, cache_key)
        if cached:
            all_rows = cached
        else:
            all_rows = await self.engine.execute(
                namespace="dimensions",
                table="dim_symbol",
                sql=f"SELECT symbol, company_name, sector, market_cap_category, is_current FROM t WHERE {where} ORDER BY symbol",
                params=params,
            )
            await self.cache.set(cache_ns, cache_key, all_rows, settings.cache_ttl_symbols)

        page = all_rows[offset: offset + first]
        has_next = len(all_rows) > offset + first
        edges = [
            SymbolEdge(node=_row_to_symbol(r), cursor=encode_cursor(offset + i))
            for i, r in enumerate(page)
        ]
        return SymbolConnection(
            edges=edges,
            page_info=PageInfo(
                has_next_page=has_next,
                has_previous_page=offset > 0,
                start_cursor=edges[0].cursor if edges else None,
                end_cursor=edges[-1].cursor if edges else None,
            ),
            total_count=len(all_rows),
        )
