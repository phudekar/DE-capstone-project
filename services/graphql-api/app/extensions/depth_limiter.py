"""Query depth limiting Strawberry extension."""

from __future__ import annotations

from typing import Any, Iterator

from graphql import DocumentNode, FieldNode, SelectionSetNode
from strawberry.extensions import SchemaExtension


def _calculate_depth(node: Any, current: int = 0) -> int:
    """Recursively compute the maximum depth of a GraphQL selection tree."""
    if isinstance(node, DocumentNode):
        if not node.definitions:
            return 0
        return max(_calculate_depth(d, current) for d in node.definitions)
    if hasattr(node, "selection_set") and node.selection_set:
        return max(
            _calculate_depth(sel, current + 1)
            for sel in node.selection_set.selections
        )
    return current


class QueryDepthLimiter(SchemaExtension):
    """Reject queries whose nesting depth exceeds ``max_depth``."""

    def __init__(self, max_depth: int = 5) -> None:
        self.max_depth = max_depth

    def on_operation(self) -> Iterator[None]:
        document = self.execution_context.graphql_document
        if document is not None:
            depth = _calculate_depth(document)
            if depth > self.max_depth:
                raise ValueError(
                    f"Query depth {depth} exceeds the maximum allowed depth of {self.max_depth}."
                )
        yield
