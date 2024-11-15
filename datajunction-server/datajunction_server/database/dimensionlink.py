"""Dimension links table."""
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from sqlalchemy import JSON, BigInteger, Enum, ForeignKey, Integer
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.dimensionlink import JoinCardinality, JoinType
from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.sql.parsing.backends.antlr4 import ast


class DimensionLink(Base):  # pylint: disable=too-few-public-methods
    """
    The join definition between a given node (source, dimension, or transform)
    and a dimension node.
    """

    __tablename__ = "dimensionlink"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    # A dimension node may be linked in multiple times to a given source, dimension,
    # or transform node, with each link referencing a different conceptual role.
    # One such example is a dimension node "default.users" that has "birth_date" and
    # "registration_date" as fields. "default.users" will be linked to the "default.date"
    # dimension twice, once per field, but each dimension link will have different roles.
    role: Mapped[Optional[str]]

    node_revision_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_dimensionlink_node_revision_id_noderevision",
            ondelete="CASCADE",
        ),
    )
    node_revision: Mapped[NodeRevision] = relationship(
        "NodeRevision",
        foreign_keys=[node_revision_id],
        back_populates="dimension_links",
    )
    dimension_id: Mapped[int] = mapped_column(
        ForeignKey(
            "node.id",
            name="fk_dimensionlink_dimension_id_node",
            ondelete="CASCADE",
        ),
    )
    dimension: Mapped[Node] = relationship(
        "Node",
        foreign_keys=[dimension_id],
        lazy="joined",
    )

    # SQL used to join the two nodes
    join_sql: Mapped[str]

    # Metadata about the join
    join_type: Mapped[Optional[JoinType]]
    join_cardinality: Mapped[JoinCardinality] = mapped_column(
        Enum(JoinCardinality),
        default=JoinCardinality.MANY_TO_ONE,
    )

    # Additional materialization settings that are needed in order to do this join
    materialization_conf: Mapped[Optional[Dict]] = mapped_column(JSON, default={})

    @classmethod
    def parse_join_type(cls, join_type: str) -> Optional[JoinType]:
        """
        Parse a join type string into an enum value.
        """
        join_type = join_type.strip().upper()
        join_mapping = {e.name: e for e in JoinType}
        for key, value in join_mapping.items():
            if key in join_type:
                return value
        return JoinType.LEFT  # pragma: no cover

    def join_sql_ast(self) -> "ast.Query":
        """
        The join query AST for this dimension link
        """
        # pylint: disable=import-outside-toplevel
        from datajunction_server.sql.parsing.backends.antlr4 import parse

        return parse(
            f"select 1 from {self.node_revision.name} "
            f"{self.join_type} join {self.dimension.name} "
            + (f"on {self.join_sql}" if self.join_sql else ""),
        )

    def joins(self) -> List["ast.Join"]:
        """
        The join ASTs for this dimension link
        """
        join_sql = self.join_sql_ast()
        return join_sql.select.from_.relations[-1].extensions  # type: ignore

    def foreign_key_mapping(self) -> Dict["ast.Column", "ast.Column"]:
        """
        If the dimension link was configured with an equality operation on the
        dimension's primary key columns to a set of foreign key columns, this method
        returns a mapping between the foreign keys on the node and the primary keys of
        the dimension based on the join SQL.
        """
        # pylint: disable=import-outside-toplevel
        from datajunction_server.sql.parsing.backends.antlr4 import ast

        # Find equality comparions (i.e., fact.order_id = dim.order_id)
        equality_comparisons = (
            [
                expr
                for expr in self.joins()[0].criteria.on.find_all(ast.BinaryOp)  # type: ignore
                if expr.op == ast.BinaryOpKind.Eq
            ]
            if self.joins()[0].criteria
            else []
        )
        mapping = {}
        for comp in equality_comparisons:
            if isinstance(comp.left, ast.Column) and isinstance(
                comp.right,
                ast.Column,
            ):  # pragma: no cover
                node_left = comp.left.name.namespace.identifier()  # type: ignore
                node_right = comp.right.name.namespace.identifier()  # type: ignore
                if node_left == self.node_revision.name:  # pragma: no cover
                    mapping[comp.right] = comp.left
                if node_right == self.node_revision.name:  # pragma: no cover
                    mapping[comp.left] = comp.right  # pragma: no cover
        return mapping

    @hybrid_property
    def foreign_keys(self) -> Dict[str, str]:
        """
        Returns a mapping from the foreign key column(s) on the origin node to
        the primary key column(s) on the dimension node. The dict values are column names.
        """
        return {
            right.identifier(): left.identifier()
            for left, right in self.foreign_key_mapping().items()
        }

    @hybrid_property
    def foreign_key_column_names(self) -> Set[str]:
        """
        Returns a set of foreign key column names
        """
        return {
            fk.replace(f"{self.node_revision.name}{SEPARATOR}", "")
            for fk in self.foreign_keys.keys()
        }

    @hybrid_property
    def foreign_keys_reversed(self):
        """
        Returns a mapping from the primary key column(s) on the dimension node to the
        foreign key column(s) on the origin node. The dict values are column names.
        """
        return {
            left.identifier(): right.identifier()
            for left, right in self.foreign_key_mapping().items()
        }
