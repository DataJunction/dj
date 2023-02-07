"""
Models for columns.
"""

from typing import List, Optional

from sqlmodel import Field, Relationship, SQLModel

from dj.models.engine import Engine


class CatalogEngines(SQLModel, table=True):  # type: ignore
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


class Catalog(SQLModel, table=True):  # type: ignore
    """
    A catalog.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    engines: List[Engine] = Relationship(
        link_model=CatalogEngines,
        sa_relationship_kwargs={
            "primaryjoin": "Catalog.id==CatalogEngines.catalog_id",
            "secondaryjoin": "Engine.id==CatalogEngines.engine_id",
        },
    )
