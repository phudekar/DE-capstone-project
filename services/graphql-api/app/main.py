"""FastAPI + Strawberry GraphQL application entry point."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import Response
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL

from app.config import settings
from app.context import get_context
from app.middleware.auth import APIKeyAuthMiddleware
from app.schema import schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── GraphQL Router ───────────────────────────────────────────────────────────

graphql_router = GraphQLRouter(
    schema,
    context_getter=get_context,
    graphiql=settings.enable_graphiql,
    subscription_protocols=[
        GRAPHQL_TRANSPORT_WS_PROTOCOL,
        GRAPHQL_WS_PROTOCOL,
    ],
)

# ─── FastAPI Application ──────────────────────────────────────────────────────

app = FastAPI(
    title="Trade Data GraphQL API",
    version="1.0.0",
    description="GraphQL serving layer for the DE capstone stock market lakehouse",
)

app.add_middleware(APIKeyAuthMiddleware)
app.include_router(graphql_router, prefix="/graphql")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "graphql-api"}


@app.get("/graphql/schema.graphql")
async def get_schema_sdl() -> Response:
    """Export the schema in SDL format for external tooling."""
    return Response(content=str(schema), media_type="text/plain")


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("GraphQL API starting on %s:%d", settings.host, settings.port)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )
