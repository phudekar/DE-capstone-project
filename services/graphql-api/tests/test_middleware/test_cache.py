"""Tests for in-memory TTL cache."""

from __future__ import annotations

import asyncio
import pytest

from app.cache.memory_cache import MemoryCache


@pytest.mark.asyncio
async def test_set_and_get():
    cache = MemoryCache()
    await cache.set("ns", {"key": "v"}, "hello", ttl=60)
    result = await cache.get("ns", {"key": "v"})
    assert result == "hello"


@pytest.mark.asyncio
async def test_miss_returns_none():
    cache = MemoryCache()
    result = await cache.get("ns", {"key": "nonexistent"})
    assert result is None


@pytest.mark.asyncio
async def test_expired_returns_none():
    cache = MemoryCache()
    await cache.set("ns", {"key": "x"}, "value", ttl=0)
    # ttl=0 means already expired
    await asyncio.sleep(0.01)
    result = await cache.get("ns", {"key": "x"})
    assert result is None


@pytest.mark.asyncio
async def test_invalidate_clears_namespace():
    cache = MemoryCache()
    await cache.set("ns", {"a": 1}, "v1", ttl=60)
    await cache.set("ns", {"b": 2}, "v2", ttl=60)
    await cache.set("other", {"c": 3}, "v3", ttl=60)

    await cache.invalidate("ns")

    assert await cache.get("ns", {"a": 1}) is None
    assert await cache.get("ns", {"b": 2}) is None
    assert await cache.get("other", {"c": 3}) == "v3"  # untouched


@pytest.mark.asyncio
async def test_different_params_different_keys():
    cache = MemoryCache()
    await cache.set("ns", {"key": "a"}, "value_a", ttl=60)
    await cache.set("ns", {"key": "b"}, "value_b", ttl=60)
    assert await cache.get("ns", {"key": "a"}) == "value_a"
    assert await cache.get("ns", {"key": "b"}) == "value_b"
