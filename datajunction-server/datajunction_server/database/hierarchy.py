"""Hierarchy database schema."""

from typing import List, Optional, Dict, Any

from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Integer,
    String,
    JSON,
    Text,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node
from datajunction_server.models.base import labelize


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

    # Relationships
    levels: Mapped[List["HierarchyLevel"]] = relationship(
        back_populates="hierarchy",
        cascade="all, delete-orphan",
        order_by="HierarchyLevel.level_order",
    )

    def __repr__(self):
        return f"<Hierarchy {self.name}>"


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
        ForeignKey("nodes.id"),
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


# Async database operations
async def get_hierarchy_by_name(
    session: AsyncSession,
    name: str,
) -> Optional[Hierarchy]:
    """Get a hierarchy by name with its levels."""
    result = await session.execute(
        select(Hierarchy)
        .options(relationship(Hierarchy.levels))
        .where(Hierarchy.name == name),
    )
    return result.scalar_one_or_none()


async def get_hierarchy_by_id(
    session: AsyncSession,
    hierarchy_id: int,
) -> Optional[Hierarchy]:
    """Get a hierarchy by ID with its levels."""
    result = await session.execute(
        select(Hierarchy)
        .options(relationship(Hierarchy.levels))
        .where(Hierarchy.id == hierarchy_id),
    )
    return result.scalar_one_or_none()


async def list_hierarchies(
    session: AsyncSession,
    limit: int = 100,
    offset: int = 0,
) -> List[Hierarchy]:
    """List all hierarchies."""
    result = await session.execute(
        select(Hierarchy)
        .options(relationship(Hierarchy.levels))
        .limit(limit)
        .offset(offset),
    )
    return list(result.scalars().all())


async def get_hierarchies_using_dimension(
    session: AsyncSession,
    dimension_node_id: int,
) -> List[Hierarchy]:
    """Get all hierarchies that use a specific dimension node."""
    result = await session.execute(
        select(Hierarchy)
        .join(HierarchyLevel)
        .options(relationship(Hierarchy.levels))
        .where(HierarchyLevel.dimension_node_id == dimension_node_id),
    )
    return list(result.scalars().all())


async def validate_hierarchy_levels(
    session: AsyncSession,
    levels: List[Dict[str, Any]],
) -> List[str]:
    """
    Validate hierarchy level definitions and return any validation errors.

    Args:
        levels: List of level definitions with dimension_node_id and optional grain_columns

    Returns:
        List of validation error messages (empty if valid)
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
