"""Agent action event model."""

from pydantic import BaseModel

from de_common.models.enums import AgentType


class AgentActionEvent(BaseModel):
    """Agent decision or action logged."""

    event_id: str
    timestamp: str
    agent_id: str
    agent_type: AgentType
    symbol: str
    action: str
    decision_factors: list[str]
