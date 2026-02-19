"""Shared enums for DE-Stock event types."""

from enum import Enum


class Side(str, Enum):
    BUY = "Buy"
    SELL = "Sell"


class OrderType(str, Enum):
    MARKET = "Market"
    LIMIT = "Limit"


class AgentType(str, Enum):
    RETAIL = "Retail"
    INSTITUTIONAL = "Institutional"
    MARKET_MAKER = "MarketMaker"
    HFT = "HFT"
    NOISE = "Noise"
    INFORMED = "Informed"


class CircuitBreakerLevel(int, Enum):
    LEVEL_1 = 1  # 7% decline, 5 min halt
    LEVEL_2 = 2  # 13% decline, 10 min halt
    LEVEL_3 = 3  # 20% decline, 15 min halt
