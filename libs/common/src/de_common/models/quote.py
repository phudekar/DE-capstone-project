"""Quote event model: QuoteUpdate."""

from pydantic import BaseModel


class QuoteUpdateEvent(BaseModel):
    """Best bid/ask price change."""

    event_id: str
    timestamp: str
    symbol: str
    best_bid: float | None = None
    best_bid_size: int = 0
    best_ask: float | None = None
    best_ask_size: int = 0
    spread: float | None = None
