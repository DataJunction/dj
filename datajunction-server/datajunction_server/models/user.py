"""
Models for users and auth
"""

from typing import List, Optional

from pydantic import BaseModel

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
    catalog: Optional[CatalogInfo]
    schema_: Optional[str]
    table: Optional[str]
    description: str = ""
    query: Optional[str] = None
    created_at: UTCDatetime
    current_version: str
    missing_table: Optional[bool] = False

    class Config:
        orm_mode = True


class UserOutput(BaseModel):
    """User information to be included in responses"""

    id: int
    username: str
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool = False
    created_collections: Optional[List[CollectionInfo]] = []
    created_nodes: Optional[List[CreatedNode]] = []
    created_tags: Optional[List[TagOutput]] = []

    class Config:
        orm_mode = True


class UserNameOnly(BaseModel):
    """
    Username only
    """

    username: str

    class Config:
        orm_mode = True


class UserActivity(BaseModel):
    """
    User activity info
    """

    username: str
    count: int
