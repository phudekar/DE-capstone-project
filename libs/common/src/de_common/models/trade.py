"""Trade event model: TradeExecuted."""

from pydantic import BaseModel

from de_common.models.enums import Side


class TradeExecutedEvent(BaseModel):
    """Two orders matched in the order book."""

    trade_id: str
    symbol: str
    price: float
    quantity: int
    buyer_order_id: str
    seller_order_id: str
    buyer_agent_id: str
    seller_agent_id: str
    aggressor_side: Side
