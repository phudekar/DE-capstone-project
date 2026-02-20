"""User context and authentication models."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class UserContext:
    user_id: str
    roles: list[str] = field(default_factory=list)
    account_id: Optional[str] = None

    def has_role(self, required: list[str]) -> bool:
        if "admin" in self.roles:
            return True
        return bool(set(self.roles) & set(required))


ANONYMOUS = UserContext(user_id="anonymous", roles=["viewer"])
