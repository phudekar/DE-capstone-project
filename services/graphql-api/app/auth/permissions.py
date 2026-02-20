"""Role-based permission helpers for GraphQL resolvers."""

import strawberry
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import GraphQLContext


def require_role(info: strawberry.types.Info, roles: list[str]) -> None:
    """Raise PermissionError if the current user lacks any of the required roles."""
    user = info.context.user
    if not user.has_role(roles):
        raise PermissionError(
            f"This operation requires one of the following roles: {roles}. "
            f"Current roles: {user.roles}"
        )
