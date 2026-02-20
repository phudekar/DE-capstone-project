"""Prometheus metrics for the GraphQL API service."""

from prometheus_client import Counter, Histogram, Gauge, REGISTRY

graphql_requests_total = Counter(
    "graphql_requests_total",
    "Total GraphQL requests",
    ["operation_type", "operation_name", "status"],
)

graphql_request_duration_seconds = Histogram(
    "graphql_request_duration_seconds",
    "GraphQL request duration in seconds",
    ["operation_type", "operation_name"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

graphql_errors_total = Counter(
    "graphql_errors_total",
    "Total GraphQL errors",
    ["error_type"],
)

graphql_active_requests = Gauge(
    "graphql_active_requests",
    "Number of in-flight GraphQL requests",
)

graphql_cache_hits_total = Counter(
    "graphql_cache_hits_total",
    "Total cache hits for query results",
)

graphql_cache_misses_total = Counter(
    "graphql_cache_misses_total",
    "Total cache misses for query results",
)
