"""Validate incoming WebSocket messages against Pydantic MarketEvent models."""

import logging
from typing import Any

from pydantic import TypeAdapter, ValidationError

from de_common.models.events import MarketEvent

logger = logging.getLogger(__name__)

_market_event_adapter = TypeAdapter(MarketEvent)


def validate_message(raw: dict[str, Any]) -> MarketEvent | None:
    """Parse and validate a raw JSON dict as a MarketEvent.

    Returns the validated envelope on success, None on validation failure.
    """
    try:
        return _market_event_adapter.validate_python(raw)
    except ValidationError as exc:
        logger.warning("Validation failed for event: %s — %s", raw.get("event_type", "unknown"), exc)
        return None
