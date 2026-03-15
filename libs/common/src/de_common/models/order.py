"""Order event models: OrderPlaced and OrderCancelled."""

from typing import Optional

from pydantic import BaseModel

from de_common.models.enums import AgentType, OrderType, Side


class OrderPlacedEvent(BaseModel):
    """A new order submitted to the order book by an agent."""

    event_id: str
    timestamp: str
    order_id: str
    symbol: str
    side: Side
    order_type: OrderType
    price: Optional[float] = None
    quantity: int
    agent_id: str
    agent_type: AgentType


class OrderCancelledEvent(BaseModel):
    """An order removed from the order book."""

    event_id: str
    timestamp: str
    order_id: str
    symbol: str
    side: Side
    price: float
    remaining_quantity: int
    agent_id: str
    reason: str
