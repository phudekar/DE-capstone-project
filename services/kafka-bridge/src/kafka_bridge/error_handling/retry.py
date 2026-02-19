"""Retry with exponential backoff for transient Kafka errors."""

import asyncio
import logging
from collections.abc import Callable
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

MAX_RETRIES = 3
BASE_DELAY = 0.5


async def retry_with_backoff(
    fn: Callable[[], T],
    max_retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    description: str = "operation",
) -> T:
    """Retry a synchronous callable with exponential backoff.

    Raises the last exception if all retries are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = base_delay * (2**attempt)
                logger.warning(
                    "Retry %d/%d for %s after error: %s (backoff %.1fs)",
                    attempt + 1,
                    max_retries,
                    description,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]
