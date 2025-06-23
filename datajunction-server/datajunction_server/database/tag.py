"""Tag database schema."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import JSON, BigInteger, Column, ForeignKey, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, joinedload, mapped_column, relationship
from sqlalchemy.sql.base import ExecutableOption

from datajunction_server.database.base import Base
from datajunction_server.database.user import User
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.models.base import labelize

if TYPE_CHECKING:
    from datajunction_server.database.node import Node


class Tag(Base):
    """
    A tag.
    """

    __tablename__ = "tag"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    tag_type: Mapped[str]
    description: Mapped[Optional[str]]
    display_name: Mapped[str] = mapped_column(  # pragma: no cover
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    created_by_id: Mapped[int] = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_tags",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
    tag_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, default={})

    nodes: Mapped[List["Node"]] = relationship(
        back_populates="tags",
        secondary="tagnoderelationship",
        primaryjoin="TagNodeRelationship.tag_id==Tag.id",
        secondaryjoin="TagNodeRelationship.node_id==Node.id",
    )

    @classmethod
    async def find_tags(
        cls,
        session: AsyncSession,
        tag_names: list[str] | None = None,
        tag_types: list[str] | None = None,
    ) -> list["Tag"]:
        """
        Find tags by name or tag type.
        """
        statement = select(Tag)
        if tag_names:
            statement = statement.where(Tag.name.in_(tag_names))
        if tag_types:
            statement = statement.where(Tag.tag_type.in_(tag_types))
        return (await session.execute(statement)).scalars().all()

    @classmethod
    async def get_tag_types(cls, session: AsyncSession) -> list[str]:
        """
        Get all unique tag types.
        """
        statement = select(Tag.tag_type).distinct()
        return (await session.execute(statement)).scalars().all()

    @classmethod
    async def list_nodes_with_tag(
        cls,
        session: AsyncSession,
        tag_name: str,
        options: List[ExecutableOption] | None = None,
    ) -> list["Node"]:
        """
        Find nodes with the tag.
        """
        statement = select(cls).where(Tag.name == tag_name)
        base_options = joinedload(Tag.nodes)
        if options:
            base_options = base_options.options(*options)
        statement = statement.options(base_options)
        tag = (await session.execute(statement)).unique().scalars().one_or_none()
        if not tag:  # pragma: no cover
            raise DJDoesNotExistException(
                message=f"A tag with name `{tag_name}` does not exist.",
                http_status_code=404,
            )
        return sorted(
            [node for node in tag.nodes if not node.deactivated_at],
            key=lambda x: x.name,
        )


class TagNodeRelationship(Base):
    """
    Join table between tags and nodes
    """

    __tablename__ = "tagnoderelationship"

    tag_id: Mapped[int] = mapped_column(
        ForeignKey("tag.id", name="fk_tagnoderelationship_tag_id_tag"),
        primary_key=True,
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey("node.id", name="fk_tagnoderelationship_node_id_node"),
        primary_key=True,
    )
