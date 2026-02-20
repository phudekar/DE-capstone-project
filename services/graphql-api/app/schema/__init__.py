"""GraphQL schema assembly."""

import strawberry
from app.schema.query import Query
from app.schema.mutation import Mutation
from app.schema.subscription import Subscription
from app.extensions.depth_limiter import QueryDepthLimiter
from app.extensions.metrics_extension import MetricsExtension

schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
    extensions=[
        QueryDepthLimiter(max_depth=5),
        MetricsExtension,
    ],
)
