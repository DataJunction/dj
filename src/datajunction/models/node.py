"""
Model for nodes.
"""

from datetime import datetime, timezone
from functools import partial
from typing import Dict, List, Optional

from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlmodel import Field, Relationship, SQLModel
from sqloxide import parse_sql

from datajunction.models.database import Column, Table
from datajunction.sql.inference import get_column_from_expression
from datajunction.sql.parse import find_nodes_by_key
from datajunction.utils import get_more_specific_type


class NodeRelationship(SQLModel, table=True):  # type: ignore
    """
    Join table for self-referential many-to-many relationships between nodes.
    """

    parent_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )
    child_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )


class Node(SQLModel, table=True):  # type: ignore
    """
    A node.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    description: str = ""

    created_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))
    updated_at: datetime = Field(default_factory=partial(datetime.now, timezone.utc))

    expression: Optional[str] = None

    tables: List[Table] = Relationship(
        back_populates="node",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    parents: List["Node"] = Relationship(
        back_populates="children",
        link_model=NodeRelationship,
        sa_relationship_kwargs=dict(
            primaryjoin="Node.id==NodeRelationship.child_id",
            secondaryjoin="Node.id==NodeRelationship.parent_id",
        ),
    )

    children: List["Node"] = Relationship(
        back_populates="parents",
        link_model=NodeRelationship,
        sa_relationship_kwargs=dict(
            primaryjoin="Node.id==NodeRelationship.parent_id",
            secondaryjoin="Node.id==NodeRelationship.child_id",
        ),
    )

    @property
    def columns(self) -> List[Column]:
        """
        Return the node schema.

        The schema is the superset of the columns across all tables, with the strictest
        type. Eg, if a node has a table with these types:

            timestamp: str
            user_id: int

        And another table with a single column:

            timestamp: datetime

        The node will have these columns:

            timestamp: datetime
            user_id: int

        """
        if self.expression:
            return self._infer_columns()

        columns: Dict[str, Column] = {}
        for table in self.tables:  # pylint: disable=not-an-iterable
            for column in table.columns:
                name = column.name
                columns[name] = Column(
                    name=name,
                    type=get_more_specific_type(columns[name].type, column.type)
                    if name in columns
                    else column.type,
                )

        return list(columns.values())

    def _infer_columns(self) -> List[Column]:  # pylint: disable=too-many-branches
        """
        Infer columns based on parent nodes.
        """
        tree = parse_sql(self.expression, dialect="ansi")

        # Use the first projection. We actually want to check that all the projections
        # produce the same columns, and raise an error if not.
        projection = next(find_nodes_by_key(tree, "projection"))

        columns = []
        for expression in projection:
            alias: Optional[str] = None
            if "UnnamedExpr" in expression:
                expression = expression["UnnamedExpr"]
            elif "ExprWithAlias" in expression:
                alias = expression["ExprWithAlias"]["alias"]["value"]
                expression = expression["ExprWithAlias"]["expr"]
            else:
                raise NotImplementedError(f"Unable to handle expression: {expression}")

            columns.append(get_column_from_expression(self.parents, expression, alias))

        # name nameless columns
        i = 0
        for column in columns:
            if column.name is None:
                column.name = f"_col{i}"
                i += 1

        return columns

    def __hash__(self):
        return hash(self.id)
