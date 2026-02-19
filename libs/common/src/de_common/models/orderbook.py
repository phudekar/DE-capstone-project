"""Order book snapshot model."""

from pydantic import BaseModel


class PriceLevel(BaseModel):
    """A single price level in the order book."""

    price: float
    quantity: int


class OrderBookSnapshot(BaseModel):
    """Full order book state at a point in time."""

    symbol: str
    bids: list[PriceLevel]
    asks: list[PriceLevel]
    sequence_number: int
