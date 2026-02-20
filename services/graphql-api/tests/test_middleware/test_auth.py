"""Tests for authentication middleware and permission helpers."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.auth.models import UserContext, ANONYMOUS
from app.auth.permissions import require_role
from app.cache.memory_cache import MemoryCache
from app.db.iceberg_duckdb import IcebergDuckDB
from strawberry.dataloader import DataLoader


def _make_mock_info(user: UserContext):
    """Build a minimal mock Strawberry Info with the given user context."""
    from app.context import GraphQLContext
    from app.services.watchlist_service import WatchlistService
    from app.streaming.kafka_consumer import KafkaConsumerFactory

    async def _noop(keys):
        return [None] * len(keys)

    ctx = GraphQLContext(
        engine=MagicMock(spec=IcebergDuckDB),
        cache=MemoryCache(),
        user=user,
        symbol_loader=DataLoader(load_fn=_noop),
        watchlist_service=WatchlistService(),
        kafka_factory=MagicMock(spec=KafkaConsumerFactory),
    )
    info = MagicMock()
    info.context = ctx
    return info


def test_require_role_passes_for_matching_role():
    user = UserContext(user_id="u1", roles=["analyst"])
    info = _make_mock_info(user)
    require_role(info, ["analyst", "admin"])  # Should not raise


def test_require_role_raises_for_missing_role():
    user = UserContext(user_id="u1", roles=["viewer"])
    info = _make_mock_info(user)
    with pytest.raises(PermissionError):
        require_role(info, ["analyst", "admin"])


def test_require_role_admin_passes_all():
    user = UserContext(user_id="u1", roles=["admin"])
    info = _make_mock_info(user)
    for role_set in [["viewer"], ["analyst"], ["trader"], ["admin"]]:
        require_role(info, role_set)  # Should not raise


def test_anonymous_has_viewer_role():
    assert "viewer" in ANONYMOUS.roles


def test_user_context_has_role():
    user = UserContext(user_id="u", roles=["analyst", "viewer"])
    assert user.has_role(["analyst"]) is True
    assert user.has_role(["admin"]) is False
    assert user.has_role(["admin", "viewer"]) is True  # intersection


class TestAPIKeyMiddleware:
    """Test the API key middleware (unit-level without a live ASGI app)."""

    def test_import(self):
        from app.middleware.auth import APIKeyAuthMiddleware, _API_KEY_USERS
        assert "viewer-key" in _API_KEY_USERS
        assert "analyst-key" in _API_KEY_USERS

    def test_viewer_key_has_viewer_role(self):
        from app.middleware.auth import _API_KEY_USERS
        user = _API_KEY_USERS["viewer-key"]
        assert "viewer" in user.roles
        assert "admin" not in user.roles

    def test_analyst_key_has_analyst_role(self):
        from app.middleware.auth import _API_KEY_USERS
        user = _API_KEY_USERS["analyst-key"]
        assert "analyst" in user.roles
