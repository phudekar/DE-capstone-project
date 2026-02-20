"""DataLoader for batching symbol lookups — prevents N+1 queries."""

from __future__ import annotations

from typing import Optional
from strawberry.dataloader import DataLoader

from app.db.iceberg_duckdb import IcebergDuckDB


def make_symbol_loader(engine: IcebergDuckDB) -> DataLoader:
    """Return a DataLoader that batch-loads Symbol dicts by ticker."""

    async def batch_load(keys: list[str]) -> list[Optional[dict]]:
        if not keys:
            return []
        placeholders = ",".join(["?" for _ in keys])
        rows = await engine.execute(
            namespace="dimensions",
            table="dim_symbol",
            sql=f"""
                SELECT symbol, company_name, sector, market_cap_category, is_current
                FROM t
                WHERE symbol IN ({placeholders})
                  AND is_current = true
            """,
            params=list(keys),
        )
        symbol_map = {r["symbol"]: r for r in rows}
        return [symbol_map.get(k) for k in keys]

    return DataLoader(load_fn=batch_load)
