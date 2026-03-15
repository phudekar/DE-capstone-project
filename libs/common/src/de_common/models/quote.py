"""Quote event model: QuoteUpdate."""

from pydantic import BaseModel


class QuoteUpdateEvent(BaseModel):
    """Best bid/ask price change."""

    event_id: str
    timestamp: str
    symbol: str
    best_bid: float
    best_bid_size: int
    best_ask: float
    best_ask_size: int
    spread: float
