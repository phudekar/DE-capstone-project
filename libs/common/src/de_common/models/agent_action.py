"""Agent action event model."""

from typing import Any

from pydantic import BaseModel

from de_common.models.enums import AgentType


class AgentActionEvent(BaseModel):
    """Agent decision or action logged."""

    agent_id: str
    agent_type: AgentType
    action: str
    symbol: str
    details: dict[str, Any]
