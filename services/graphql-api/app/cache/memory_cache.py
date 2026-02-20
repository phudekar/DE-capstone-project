"""Simple in-memory TTL cache, thread-safe via asyncio lock."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any


class MemoryCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[Any, float]] = {}  # key -> (value, expiry_ts)
        self._lock = asyncio.Lock()

    def _key(self, namespace: str, params: dict) -> str:
        raw = json.dumps(params, sort_keys=True, default=str).encode()
        return f"{namespace}:{hashlib.md5(raw).hexdigest()}"

    async def get(self, namespace: str, params: dict) -> Any | None:
        async with self._lock:
            key = self._key(namespace, params)
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._store[key]
                return None
            return value

    async def set(self, namespace: str, params: dict, value: Any, ttl: int) -> None:
        async with self._lock:
            key = self._key(namespace, params)
            self._store[key] = (value, time.monotonic() + ttl)

    async def invalidate(self, namespace: str) -> None:
        async with self._lock:
            prefix = f"{namespace}:"
            to_delete = [k for k in self._store if k.startswith(prefix)]
            for k in to_delete:
                del self._store[k]


# Singleton
_cache: MemoryCache | None = None


def get_cache() -> MemoryCache:
    global _cache
    if _cache is None:
        _cache = MemoryCache()
    return _cache
