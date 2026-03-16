"""Role-based permission helpers for GraphQL resolvers."""

from typing import TYPE_CHECKING

import strawberry

if TYPE_CHECKING:
    pass


def require_role(info: strawberry.types.Info, roles: list[str]) -> None:
    """Raise PermissionError if the current user lacks any of the required roles."""
    user = info.context.user
    if not user.has_role(roles):
        raise PermissionError(
            f"This operation requires one of the following roles: {roles}. Current roles: {user.roles}"
        )
