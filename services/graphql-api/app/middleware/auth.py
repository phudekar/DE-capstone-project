"""API key authentication middleware."""

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.auth.models import UserContext, ANONYMOUS
from app.config import settings


# Predefined API keys mapping to user contexts for local dev
_API_KEY_USERS: dict[str, UserContext] = {
    settings.api_key: UserContext(
        user_id="admin-user",
        roles=["admin", "trader", "analyst", "viewer"],
        account_id="ACC-001",
    ),
    "viewer-key": UserContext(
        user_id="viewer-user",
        roles=["viewer"],
        account_id="ACC-002",
    ),
    "analyst-key": UserContext(
        user_id="analyst-user",
        roles=["analyst", "viewer"],
        account_id="ACC-003",
    ),
}


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """
    Validates the X-API-Key header.
    Falls back to anonymous (viewer) if auth is disabled or no key is provided.
    """

    async def dispatch(self, request: Request, call_next):
        if not settings.auth_enabled:
            request.state.user = ANONYMOUS
            return await call_next(request)

        # GraphiQL playground pass-through in development
        if request.url.path == "/graphql" and request.method == "GET":
            request.state.user = ANONYMOUS
            return await call_next(request)

        api_key = request.headers.get("X-API-Key", "")
        user = _API_KEY_USERS.get(api_key, ANONYMOUS)
        request.state.user = user
        return await call_next(request)
