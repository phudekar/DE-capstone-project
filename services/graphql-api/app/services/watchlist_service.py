"""In-memory watchlist CRUD service (no external DB dependency for local dev)."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Optional

import strawberry

from app.schema.types import Watchlist


class WatchlistService:
    def __init__(self) -> None:
        self._store: dict[str, Watchlist] = {}
        self._lock = asyncio.Lock()

    async def create(self, user_id: str, name: str, symbols: list[str]) -> Watchlist:
        async with self._lock:
            wl = Watchlist(
                id=strawberry.ID(str(uuid.uuid4())),
                name=name,
                user_id=user_id,
                symbols=list(symbols),
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self._store[str(wl.id)] = wl
            return wl

    async def get(self, watchlist_id: str) -> Optional[Watchlist]:
        return self._store.get(watchlist_id)

    async def list_for_user(self, user_id: str) -> list[Watchlist]:
        return [w for w in self._store.values() if w.user_id == user_id]

    async def add_symbols(self, watchlist_id: str, symbols: list[str]) -> Optional[Watchlist]:
        async with self._lock:
            wl = self._store.get(watchlist_id)
            if not wl:
                return None
            combined = list(dict.fromkeys(wl.symbols + symbols))
            updated = Watchlist(
                id=wl.id,
                name=wl.name,
                user_id=wl.user_id,
                symbols=combined,
                created_at=wl.created_at,
                updated_at=datetime.now(timezone.utc),
            )
            self._store[watchlist_id] = updated
            return updated

    async def remove_symbols(self, watchlist_id: str, symbols: list[str]) -> Optional[Watchlist]:
        async with self._lock:
            wl = self._store.get(watchlist_id)
            if not wl:
                return None
            remaining = [s for s in wl.symbols if s not in symbols]
            updated = Watchlist(
                id=wl.id,
                name=wl.name,
                user_id=wl.user_id,
                symbols=remaining,
                created_at=wl.created_at,
                updated_at=datetime.now(timezone.utc),
            )
            self._store[watchlist_id] = updated
            return updated

    async def delete(self, watchlist_id: str) -> bool:
        async with self._lock:
            if watchlist_id in self._store:
                del self._store[watchlist_id]
                return True
            return False


# Singleton
_service: WatchlistService | None = None


def get_watchlist_service() -> WatchlistService:
    global _service
    if _service is None:
        _service = WatchlistService()
    return _service
