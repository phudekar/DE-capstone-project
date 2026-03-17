"""Order book snapshot model."""

from pydantic import BaseModel


class PriceLevel(BaseModel):
    """A single price level in the order book."""

    price: float
    quantity: int
    order_count: int = 0


class OrderBookSnapshot(BaseModel):
    """Full order book state at a point in time."""

    event_id: str
    timestamp: str
    symbol: str
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    sequence_number: int | None = None
