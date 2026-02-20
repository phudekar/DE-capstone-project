"""GraphQL Mutation root type."""

from __future__ import annotations

import strawberry
from strawberry.types import Info
from typing import Optional

from app.auth.permissions import require_role
from app.schema.types import Watchlist
from app.schema.inputs import WatchlistInput, AddToWatchlistInput


@strawberry.type
class Mutation:

    @strawberry.mutation(description="Create a new watchlist for the authenticated user.")
    async def create_watchlist(self, info: Info, input: WatchlistInput) -> Watchlist:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        wl_service = info.context.watchlist_service
        return await wl_service.create(
            user_id=info.context.user.user_id,
            name=input.name,
            symbols=input.symbols,
        )

    @strawberry.mutation(description="Add symbols to an existing watchlist.")
    async def add_to_watchlist(self, info: Info, input: AddToWatchlistInput) -> Optional[Watchlist]:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        wl_service = info.context.watchlist_service
        wl = await wl_service.get(str(input.watchlist_id))
        if not wl:
            raise ValueError(f"Watchlist {input.watchlist_id} not found.")
        if wl.user_id != info.context.user.user_id:
            raise PermissionError("Cannot modify another user's watchlist.")
        return await wl_service.add_symbols(str(input.watchlist_id), input.symbols)

    @strawberry.mutation(description="Remove symbols from a watchlist.")
    async def remove_from_watchlist(
        self, info: Info, watchlist_id: strawberry.ID, symbols: list[str]
    ) -> Optional[Watchlist]:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        wl_service = info.context.watchlist_service
        wl = await wl_service.get(str(watchlist_id))
        if not wl:
            raise ValueError(f"Watchlist {watchlist_id} not found.")
        if wl.user_id != info.context.user.user_id:
            raise PermissionError("Cannot modify another user's watchlist.")
        return await wl_service.remove_symbols(str(watchlist_id), symbols)

    @strawberry.mutation(description="Delete a watchlist.")
    async def delete_watchlist(self, info: Info, watchlist_id: strawberry.ID) -> bool:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        wl_service = info.context.watchlist_service
        wl = await wl_service.get(str(watchlist_id))
        if not wl:
            raise ValueError(f"Watchlist {watchlist_id} not found.")
        if wl.user_id != info.context.user.user_id:
            raise PermissionError("Cannot delete another user's watchlist.")
        return await wl_service.delete(str(watchlist_id))

    @strawberry.mutation(description="List watchlists for the authenticated user.")
    async def my_watchlists(self, info: Info) -> list[Watchlist]:
        require_role(info, ["viewer", "analyst", "trader", "admin"])
        return await info.context.watchlist_service.list_for_user(info.context.user.user_id)
