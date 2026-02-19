"""MarketEvent — Discriminated union of all DE-Stock event types."""

from datetime import datetime
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field

from de_common.models.agent_action import AgentActionEvent
from de_common.models.market_stats import MarketStatsEvent
from de_common.models.order import OrderCancelledEvent, OrderPlacedEvent
from de_common.models.orderbook import OrderBookSnapshot
from de_common.models.quote import QuoteUpdateEvent
from de_common.models.trade import TradeExecutedEvent
from de_common.models.trading_halt import TradingHaltEvent, TradingResumeEvent


class OrderPlacedEnvelope(BaseModel):
    event_type: Literal["OrderPlaced"]
    timestamp: datetime
    data: OrderPlacedEvent


class TradeExecutedEnvelope(BaseModel):
    event_type: Literal["TradeExecuted"]
    timestamp: datetime
    data: TradeExecutedEvent


class QuoteUpdateEnvelope(BaseModel):
    event_type: Literal["QuoteUpdate"]
    timestamp: datetime
    data: QuoteUpdateEvent


class OrderBookSnapshotEnvelope(BaseModel):
    event_type: Literal["OrderBookSnapshot"]
    timestamp: datetime
    data: OrderBookSnapshot


class TradingHaltEnvelope(BaseModel):
    event_type: Literal["TradingHalt"]
    timestamp: datetime
    data: TradingHaltEvent


class TradingResumeEnvelope(BaseModel):
    event_type: Literal["TradingResume"]
    timestamp: datetime
    data: TradingResumeEvent


class AgentActionEnvelope(BaseModel):
    event_type: Literal["AgentAction"]
    timestamp: datetime
    data: AgentActionEvent


class MarketStatsEnvelope(BaseModel):
    event_type: Literal["MarketStats"]
    timestamp: datetime
    data: MarketStatsEvent


class OrderCancelledEnvelope(BaseModel):
    event_type: Literal["OrderCancelled"]
    timestamp: datetime
    data: OrderCancelledEvent


MarketEvent = Annotated[
    Union[
        OrderPlacedEnvelope,
        TradeExecutedEnvelope,
        QuoteUpdateEnvelope,
        OrderBookSnapshotEnvelope,
        TradingHaltEnvelope,
        TradingResumeEnvelope,
        AgentActionEnvelope,
        MarketStatsEnvelope,
        OrderCancelledEnvelope,
    ],
    Field(discriminator="event_type"),
]
