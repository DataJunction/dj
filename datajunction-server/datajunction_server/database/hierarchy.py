"""Hierarchy database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    JSON,
    Text,
    select,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.dimensionlink import JoinCardinality
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.base import labelize
from datajunction_server.typing import UTCDatetime
from datajunction_server.models.hierarchy import HierarchyLevelInput

if TYPE_CHECKING:
    from datajunction_server.database.history import History


class Hierarchy(Base):  # type: ignore
    """
    Database model for dimensional hierarchies.

    A hierarchy defines an ordered set of dimension nodes that form
    a hierarchical relationship for drill-down/roll-up operations.
    """

    __tablename__ = "hierarchies"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    display_name: Mapped[Optional[str]] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Audit fields
    created_by_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"),
        nullable=False,
    )
    created_by: Mapped[User] = relationship(
        "User",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    # Relationships
    levels: Mapped[List["HierarchyLevel"]] = relationship(
        back_populates="hierarchy",
        cascade="all, delete-orphan",
        order_by="HierarchyLevel.level_order",
    )

    history: Mapped[List["History"]] = relationship(
        primaryjoin="History.entity_name==Hierarchy.name",
        order_by="History.created_at",
        foreign_keys="History.entity_name",
        viewonly=True,
    )

    def __repr__(self):
        return f"<Hierarchy {self.name}>"

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
    ) -> Optional["Hierarchy"]:
        """Get a hierarchy by name with its levels loaded."""
        result = await session.execute(
            select(Hierarchy)
            .options(
                selectinload(Hierarchy.levels).selectinload(
                    HierarchyLevel.dimension_node,
                ),
                selectinload(Hierarchy.created_by),
            )
            .where(Hierarchy.name == name),
        )
        return result.scalar_one_or_none()

    @classmethod
    async def get_by_id(
        cls,
        session: AsyncSession,
        hierarchy_id: int,
    ) -> Optional["Hierarchy"]:
        """Get a hierarchy by ID with its levels loaded."""
        result = await session.execute(
            select(Hierarchy)
            .options(
                selectinload(Hierarchy.levels).selectinload(
                    HierarchyLevel.dimension_node,
                ),
                selectinload(Hierarchy.created_by),
            )
            .where(cls.id == hierarchy_id),
        )
        return result.scalar_one_or_none()

    @classmethod
    async def list_all(
        cls,
        session: AsyncSession,
        limit: int = 100,
        offset: int = 0,
    ) -> List["Hierarchy"]:
        """List all hierarchies with their levels and created_by info loaded."""
        result = await session.execute(
            select(cls)
            .options(
                selectinload(cls.levels),
                selectinload(cls.created_by),
            )
            .limit(limit)
            .offset(offset),
        )
        return list(result.scalars().all())

    @classmethod
    async def get_using_dimension(
        cls,
        session: AsyncSession,
        dimension_node_id: int,
    ) -> List["Hierarchy"]:
        """Get all hierarchies that use a specific dimension node."""
        result = await session.execute(
            select(cls)
            .join(HierarchyLevel)
            .options(selectinload(cls.levels))
            .where(HierarchyLevel.dimension_node_id == dimension_node_id),
        )
        return list(result.scalars().all())

    @classmethod
    async def validate_levels(
        cls,
        session: AsyncSession,
        levels: list[HierarchyLevelInput],
    ) -> tuple[list[str], dict[str, Node]]:
        """
        Validate hierarchy level definitions and return any validation errors.
        """
        errors = []
        existing_nodes: dict[str, Node] = {}

        # Check for unique level names
        names = [level.name for level in levels]
        if len(set(names)) != len(names):
            errors.append("Level names must be unique")

        # Resolve dimension node names to IDs and validate they exist (single DB call)
        dimension_node_names = [level.dimension_node for level in levels]
        existing_nodes = {
            node.name: node
            for node in await Node.get_by_names(session, dimension_node_names)
        }

        # Check each level's dimension node and resolve to IDs
        for level in levels:
            node_name = level.dimension_node
            if node_name not in existing_nodes:
                errors.append(
                    f"Level '{level.name}': Dimension node '{node_name}' does not exist",
                )
                continue

            if existing_nodes[node_name].type != NodeType.DIMENSION:
                errors.append(
                    f"Level '{level.name}': Node '{node_name}' "
                    f"is not a dimension node (type: {existing_nodes[node_name].type})",
                )
                continue

        # For multi-dimension hierarchies, validate dimension FK relationships
        for i in range(len(levels) - 1):
            current_level = levels[i]
            next_level = levels[i + 1]

            # Skip if nodes are the same (single-dimension hierarchy section)
            if current_level.dimension_node == next_level.dimension_node:
                continue

            parent_node = existing_nodes.get(current_level.dimension_node)
            child_node = existing_nodes.get(next_level.dimension_node)
            # Check if there's a valid dimension link from child to parent
            if child_node and parent_node:
                valid_link, link_errors = await cls.has_valid_hierarchy_link_to(
                    session,
                    child_node=child_node,
                    parent_node=parent_node,
                )
                errors += link_errors
        return errors, existing_nodes

    @classmethod
    async def has_valid_hierarchy_link_to(
        cls,
        session: AsyncSession,
        child_node: Node,
        parent_node: Node,
    ) -> tuple[bool, list[str]]:
        """
        Validate hierarchical dimension link between child and parent nodes.
        """
        # Get dimension links
        result = await session.execute(
            select(
                DimensionLink.join_cardinality,
                DimensionLink.join_type,
                DimensionLink.join_sql,
            )
            .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
            .where(
                NodeRevision.node_id == child_node.id,
                DimensionLink.dimension_id == parent_node.id,
            ),
        )

        links = result.all()
        messages = []

        if not links:
            return False, [
                f"No dimension link exists between {child_node.name} and {parent_node.name}",
            ]

        # Validate cardinality (hard requirement)
        valid_links = [link for link in links if link[0] == JoinCardinality.MANY_TO_ONE]
        if not valid_links:
            return False, ["Invalid cardinality (expected MANY_TO_ONE)"]

        # Check if join_sql references parent's primary key
        for cardinality, join_type, join_sql in valid_links:
            join_sql_lower = join_sql.lower()
            parent_name_lower = parent_node.name.lower()

            # Simple heuristic: check if PK columns are mentioned in join
            parent_pk_columns = [col.name for col in parent_node.current.primary_key()]
            print(
                "searching for pk cols",
                [
                    f"{parent_name_lower}.{pk_col.lower()}"
                    for pk_col in parent_pk_columns
                ],
                "in",
                join_sql_lower,
            )
            pk_referenced = any(
                f"{parent_name_lower}.{pk_col.lower()}" in join_sql_lower
                for pk_col in parent_pk_columns
            )

            if not pk_referenced:
                messages.append(
                    f"WARN: Join SQL may not reference parent's primary key columns {parent_pk_columns}. "
                    f"Join: {join_sql}",
                )

        return True, messages


class HierarchyLevel(Base):  # type: ignore
    """
    Database model for individual levels within a hierarchy.

    Each level references a dimension node and defines its position
    in the hierarchical ordering.
    """

    __tablename__ = "hierarchy_levels"
    __table_args__ = (UniqueConstraint("hierarchy_id", "name"),)

    id: Mapped[int] = mapped_column(BigInteger(), primary_key=True)

    hierarchy_id: Mapped[int] = mapped_column(
        BigInteger(),
        ForeignKey("hierarchies.id"),
        nullable=False,
    )
    hierarchy: Mapped["Hierarchy"] = relationship(back_populates="levels")

    name: Mapped[str] = mapped_column(String, nullable=False)

    dimension_node_id: Mapped[int] = mapped_column(
        BigInteger(),
        ForeignKey("node.id"),
        nullable=False,
    )
    dimension_node: Mapped["Node"] = relationship("Node")

    level_order: Mapped[int] = mapped_column(Integer, nullable=False)

    # Optional: For single-dimension hierarchies where multiple levels
    # reference the same dimension node but use different grain columns
    grain_columns: Mapped[Optional[List[str]]] = mapped_column(JSON)

    def __repr__(self):
        return f"<HierarchyLevel {self.name} (order={self.level_order})>"
