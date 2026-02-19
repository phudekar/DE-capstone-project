"""Trading halt and resume event models."""

from pydantic import BaseModel

from de_common.models.enums import CircuitBreakerLevel


class TradingHaltEvent(BaseModel):
    """Circuit breaker triggered — trading halted."""

    symbol: str
    reason: str
    level: CircuitBreakerLevel
    trigger_price: float
    reference_price: float
    decline_pct: float
    halt_duration_secs: int


class TradingResumeEvent(BaseModel):
    """Trading resumed after a halt."""

    symbol: str
    halted_at: str
    halt_duration_secs: int
    level: CircuitBreakerLevel
