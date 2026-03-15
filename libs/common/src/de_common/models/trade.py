"""Trade event model: TradeExecuted."""

from pydantic import BaseModel


class TradeExecutedEvent(BaseModel):
    """Two orders matched in the order book."""

    event_id: str
    timestamp: str
    trade_id: str
    symbol: str
    price: float
    quantity: int
    buy_order_id: str
    sell_order_id: str
    buyer_agent_id: str
    seller_agent_id: str
    is_aggressive_buy: bool
