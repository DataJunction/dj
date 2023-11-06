"""
Models for columns.
"""
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import DateTime
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy_utils import UUIDType
from sqlmodel import JSON, Field, Relationship, SQLModel

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.engine import Engine, EngineInfo
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.models import NodeRevision, Table


class Catalog(BaseSQLModel, table=True):  # type: ignore
    """
    A catalog.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: UUID = Field(default_factory=uuid4, sa_column=SqlaColumn(UUIDType()))
    name: str
    # engines: List[Engine] = Relationship(
    #     link_model=CatalogEngines,
    #     sa_relationship_kwargs={
    #         "primaryjoin": "Catalog.id==CatalogEngines.catalog_id",
    #         "secondaryjoin": "Engine.id==CatalogEngines.engine_id",
    #     },
    # )
    node_revisions: List["NodeRevision"] = Relationship(back_populates="catalog")
    created_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    updated_at: UTCDatetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    extra_params: Dict = Field(default={}, sa_column=SqlaColumn(JSON))

    def get_available_engines(self, query_service_client: QueryServiceClient) -> List[EngineInfo]:
        return query_service_client.get_available_engines(catalog=self.name)

    def __str__(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(self.id)


class CatalogInfo(SQLModel):
    """
    Class for catalog creation
    """

    name: str
    engines: List[EngineInfo] = []
