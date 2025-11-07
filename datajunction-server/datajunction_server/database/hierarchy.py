"""Hierarchy database schema."""

from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, List, Optional, Dict, Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Integer,
    String,
    JSON,
    Text,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.models.base import labelize
from datajunction_server.typing import UTCDatetime

# Import for type checking to avoid circular imports
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
            select(cls).options(selectinload(cls.levels)).where(cls.name == name),
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
            select(cls).options(selectinload(cls.levels)).where(cls.id == hierarchy_id),
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
        levels: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Validate hierarchy level definitions and return any validation errors.
        """
        errors = []

        if len(levels) < 2:
            errors.append("Hierarchy must have at least 2 levels")
            return errors

        # Check for unique level orders
        orders = [level.get("level_order") for level in levels]
        if len(set(orders)) != len(orders):
            errors.append("Level orders must be unique")

        # Check for unique level names
        names = [level.get("name") for level in levels]
        if len(set(names)) != len(names):
            errors.append("Level names must be unique")

        # Validate dimension nodes exist
        dimension_node_ids = [level.get("dimension_node_id") for level in levels]
        result = await session.execute(
            select(Node.id).where(Node.id.in_(dimension_node_ids)),
        )
        existing_ids = {row[0] for row in result.all()}

        for level in levels:
            node_id = level.get("dimension_node_id")
            if node_id not in existing_ids:
                errors.append(f"Dimension node with ID {node_id} does not exist")

        # For multi-dimension hierarchies, validate dimension links exist
        # (This is a simplified check - in practice you'd want to verify actual FK relationships)
        dimension_nodes = set(dimension_node_ids)
        if len(dimension_nodes) > 1:
            # TODO: Add validation for dimension links between consecutive levels
            # This would check that level[i+1] has a dimension link to level[i]
            pass

        return errors


class HierarchyLevel(Base):  # type: ignore
    """
    Database model for individual levels within a hierarchy.

    Each level references a dimension node and defines its position
    in the hierarchical ordering.
    """

    __tablename__ = "hierarchy_levels"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    hierarchy_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey("hierarchies.id"),
        nullable=False,
    )
    hierarchy: Mapped["Hierarchy"] = relationship(back_populates="levels")

    name: Mapped[str] = mapped_column(String, nullable=False)

    dimension_node_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey("node.id"),
        nullable=False,
    )
    dimension_node: Mapped["Node"] = relationship("Node")

    level_order: Mapped[int] = mapped_column(Integer, nullable=False)

    # Optional: For single-dimension hierarchies where multiple levels
    # reference the same dimension node but use different grain columns
    grain_columns: Mapped[Optional[List[str]]] = mapped_column(JSON)

    # Unique constraints
    __table_args__ = ({"sqlite_autoincrement": True},)

    def __repr__(self):
        return f"<HierarchyLevel {self.name} (order={self.level_order})>"
