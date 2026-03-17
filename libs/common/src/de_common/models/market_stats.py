"""Market statistics event model."""

from pydantic import BaseModel


class MarketStatsEvent(BaseModel):
    """Periodic market statistics for a symbol."""

    event_id: str
    timestamp: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    trade_count: int
    vwap: float
