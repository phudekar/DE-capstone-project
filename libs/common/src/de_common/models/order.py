"""Order event models: OrderPlaced and OrderCancelled."""

from datetime import datetime

from pydantic import BaseModel

from de_common.models.enums import AgentType, OrderType, Side


class OrderPlacedEvent(BaseModel):
    """A new order submitted to the order book by an agent."""

    order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    price: float
    quantity: int
    agent_id: str
    agent_type: AgentType


class OrderCancelledEvent(BaseModel):
    """An order removed from the order book."""

    order_id: str
    symbol: str
    side: Side
    price: float
    remaining_quantity: int
    agent_id: str
    reason: str
