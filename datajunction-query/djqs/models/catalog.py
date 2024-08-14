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

from djqs.models.engine import BaseEngineInfo, QSEngine

if TYPE_CHECKING:
    from djqs.utils import UTCDatetime


class QSCatalogEngines(SQLModel, table=True):  # type: ignore
    """
    Join table for catalogs and engines.
    """

    catalog_id: Optional[int] = Field(
        default=None,
        foreign_key="catalog.id",
        primary_key=True,
    )
    engine_id: Optional[int] = Field(
        default=None,
        foreign_key="engine.id",
        primary_key=True,
    )


class QSCatalog(SQLModel, table=True):  # type: ignore
    """
    A QScatalog.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    uuid: UUID = Field(default_factory=uuid4, sa_column=SqlaColumn(UUIDType()))
    name: str
    engines: List[QSEngine] = Relationship(
        link_model=QSCatalogEngines,
        sa_relationship_kwargs={
            "primaryjoin": "Catalog.id==QSCatalogEngines.catalog_id",
            "secondaryjoin": "Engine.id==QSCatalogEngines.engine_id",
        },
    )
    created_at: "UTCDatetime" = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    updated_at: "UTCDatetime" = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    extra_params: Dict = Field(default={}, sa_column=SqlaColumn(JSON))

    def __str__(self) -> str:
        return self.name  # pragma: no cover

    def __hash__(self) -> int:
        return hash(self.id)  # pragma: no cover


class CatalogInfo(SQLModel):
    """
    Class for catalog creation
    """

    name: str
    engines: List[BaseEngineInfo] = []
