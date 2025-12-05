"""
Node to dimension reachability table.

This table stores pairs of (source_node_id, target_dimension_node_id), representing
that the source node (of any type) can reach/filter by the target dimension node.
This allows O(1) lookups for "which nodes can use dimension X?" queries.
"""

from sqlalchemy import BigInteger, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base


class DimensionReachability(Base):
    """
    Pre-computed node-to-dimension reachability.

    A row (source_node_id, target_dimension_node_id) means: source_node can
    filter by / has access to target_dimension_node.

    Source nodes can be of any type (metric, source, transform, dimension, cube).
    Target nodes are always dimensions.
    """

    __tablename__ = "dimension_reachability"
    __table_args__ = (
        UniqueConstraint(
            "source_node_id",
            "target_dimension_node_id",
            name="uq_dimension_reachability_source_target",
        ),
        Index("idx_dimension_reachability_source", "source_node_id"),
        Index("idx_dimension_reachability_target", "target_dimension_node_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    source_node_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "node.id",
            name="fk_dimension_reachability_source_node_id",
            ondelete="CASCADE",
        ),
    )
    source_node = relationship(
        "Node",
        foreign_keys=[source_node_id],
    )

    target_dimension_node_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "node.id",
            name="fk_dimension_reachability_target_node_id",
            ondelete="CASCADE",
        ),
    )
    target_dimension_node = relationship(
        "Node",
        foreign_keys=[target_dimension_node_id],
    )
