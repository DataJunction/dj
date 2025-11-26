"""
Models for groups
"""

from pydantic import BaseModel, ConfigDict

from datajunction_server.typing import UTCDatetime


class GroupOutput(BaseModel):
    """Group information to be included in responses"""

    id: int
    username: str
    email: str | None = None
    name: str | None = None
    created_at: UTCDatetime | None = None

    model_config = ConfigDict(from_attributes=True)
