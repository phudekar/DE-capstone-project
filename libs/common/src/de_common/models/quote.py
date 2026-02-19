"""Quote event model: QuoteUpdate."""

from pydantic import BaseModel


class QuoteUpdateEvent(BaseModel):
    """Best bid/ask price change."""

    symbol: str
    bid_price: float
    bid_size: int
    ask_price: float
    ask_size: int
    spread: float
    mid_price: float
