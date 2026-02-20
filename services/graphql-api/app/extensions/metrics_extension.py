"""Strawberry schema extension that records Prometheus metrics per request."""

import time
from typing import Iterator

from strawberry.extensions import SchemaExtension

from app.metrics import (
    graphql_requests_total,
    graphql_request_duration_seconds,
    graphql_errors_total,
    graphql_active_requests,
)


class MetricsExtension(SchemaExtension):
    """Record request counts, durations, and errors for every GraphQL operation."""

    def on_operation(self) -> Iterator[None]:
        start = time.perf_counter()
        graphql_active_requests.inc()
        op = self.execution_context.operation_name or "anonymous"
        op_type = "unknown"
        if self.execution_context.query:
            q = self.execution_context.query.strip().lower()
            if q.startswith("mutation"):
                op_type = "mutation"
            elif q.startswith("subscription"):
                op_type = "subscription"
            else:
                op_type = "query"

        try:
            yield
            result = self.execution_context.result
            status = "error" if (result and result.errors) else "success"
            if result and result.errors:
                for err in result.errors:
                    graphql_errors_total.labels(
                        error_type=type(err.original_error).__name__
                        if err.original_error
                        else "GraphQLError"
                    ).inc()
        except Exception as exc:
            status = "error"
            graphql_errors_total.labels(error_type=type(exc).__name__).inc()
            raise
        finally:
            elapsed = time.perf_counter() - start
            graphql_active_requests.dec()
            graphql_requests_total.labels(
                operation_type=op_type,
                operation_name=op,
                status=status,
            ).inc()
            graphql_request_duration_seconds.labels(
                operation_type=op_type,
                operation_name=op,
            ).observe(elapsed)
