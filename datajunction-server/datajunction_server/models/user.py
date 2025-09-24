"""
Models for users and auth
"""

from pydantic import BaseModel, ConfigDict, Field

from datajunction_server.database.user import OAuthProvider
from datajunction_server.models.catalog import CatalogInfo
from datajunction_server.models.collection import CollectionInfo
from datajunction_server.models.node import NodeType
from datajunction_server.models.tag import TagOutput
from datajunction_server.typing import UTCDatetime


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
    created_collections: list[CollectionInfo] = Field(default_factory=list)
    created_nodes: list[CreatedNode] = Field(default_factory=list)
    owned_nodes: list[CreatedNode] = Field(default_factory=list)
    created_tags: list[TagOutput] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class UserNameOnly(BaseModel):
    """
    Username only
    """

    username: str

    model_config = ConfigDict(from_attributes=True)


class UserActivity(BaseModel):
    """
    User activity info
    """

    username: str
    count: int
