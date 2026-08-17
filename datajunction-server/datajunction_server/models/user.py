"""
Models for users and auth
"""

from pydantic import BaseModel, ConfigDict

from datajunction_server.database.user import OAuthProvider
from datajunction_server.models.catalog import CatalogInfo
from datajunction_server.models.node import NodeType

# Declared in `models/principal.py`, which stays importable from the query-service
# payload models; re-exported here, where callers have always imported them from.
from datajunction_server.models.principal import (
    PrincipalKind,
    PrincipalRef,
    UserNameOnly,
)
from datajunction_server.typing import UTCDatetime

__all__ = [
    "CreatedNode",
    "OAuthProvider",
    "PrincipalKind",
    "PrincipalRef",
    "UserActivity",
    "UserNameOnly",
    "UserOutput",
]


class CreatedNode(BaseModel):
    """
    A node created by a user
    """

    namespace: str
    type: NodeType
    name: str
    catalog: CatalogInfo | None = None
    schema_: str | None = None
    table: str | None = None
    description: str = ""
    query: str | None = None
    created_at: UTCDatetime
    current_version: str
    missing_table: bool | None = False

    model_config = ConfigDict(from_attributes=True)


class UserOutput(BaseModel):
    """User information to be included in responses"""

    id: int
    username: str
    email: str | None = None
    name: str | None = None
    oauth_provider: OAuthProvider
    is_admin: bool = False
    last_viewed_notifications_at: UTCDatetime | None = None

    model_config = ConfigDict(from_attributes=True)


class UserActivity(BaseModel):
    """
    User activity info
    """

    username: str
    count: int
