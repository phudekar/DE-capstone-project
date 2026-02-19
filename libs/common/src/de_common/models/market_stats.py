"""Market statistics event model."""

from pydantic import BaseModel


class MarketStatsEvent(BaseModel):
    """Periodic market statistics for a symbol."""

    symbol: str
    open: float
    high: float
    low: float
    last: float
    volume: int
    trade_count: int
    vwap: float
    turnover: float
