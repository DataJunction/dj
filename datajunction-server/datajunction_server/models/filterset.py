from typing import TYPE_CHECKING, Optional

from sqlalchemy.sql.schema import UniqueConstraint
from sqlmodel import Field, Relationship, SQLModel

from datajunction_server.models.base import BaseSQLModel

if TYPE_CHECKING:
    from datajunction_server.models import NodeRevision

# class NodeFilterset(BaseSQLModel, table=True):  # type: ignore
#     """
#     Join table for filtersets
#     """
#
#     filterset_id: Optional[int] = Field(
#         default=None,
#         foreign_key="filterset.id",
#         primary_key=True,
#     )
#     node_revision_id: Optional[int] = Field(
#         default=None,
#         foreign_key="noderevision.id",
#         primary_key=True,
#     )


class FiltersetBase(BaseSQLModel):
    """
    Filterset
    """

    # The name of the filterset
    name: str

    # SQL expression that represents the filters on this cube
    filters: str


class Filterset(FiltersetBase, table=True):  # type: ignore
    """
    Filterset table
    """

    __table_args__ = (
        UniqueConstraint(
            "name",
            "node_revision_id",
            name="unique_name_node_revision_id",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    node_revision_id: Optional[int] = Field(foreign_key="noderevision.id")
    node_revision: "NodeRevision" = Relationship(
        # link_model=NodeFilterset,
        # sa_relationship_kwargs={
        #     "primaryjoin": "NodeFilterset.node_revision_id==NodeRevision.id",
        #     "secondaryjoin": "NodeFilterset.filterset_id==Filterset.id",
        #     "cascade": "all, delete",
        #     "uselist": False,
        # },
        back_populates="filtersets",
    )
