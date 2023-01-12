"""
Model for nodes.
"""

import enum
from collections import defaultdict
from datetime import datetime, timezone
from functools import partial
from typing import Dict, List, Optional, TypedDict, cast

from sqlalchemy import DateTime, String
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Enum
from sqlmodel import Field, Relationship, SQLModel

from dj.models.column import Column, ColumnYAML
from dj.models.table import Table, TableYAML
from dj.sql.parse import is_metric


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


class NodeColumns(SQLModel, table=True):  # type: ignore
    """
    Join table for node columns.
    """

    node_id: Optional[int] = Field(
        default=None,
        foreign_key="node.id",
        primary_key=True,
    )
    column_id: Optional[int] = Field(
        default=None,
        foreign_key="column.id",
        primary_key=True,
    )


class NodeType(str, enum.Enum):
    """
    Node type.

    A node can have 4 types, currently:

    1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.
    2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.
    3. METRIC nodes are leaves in the DAG, and have a single aggregation query.
    4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.
    """

    SOURCE = "source"
    TRANSFORM = "transform"
    METRIC = "metric"
    DIMENSION = "dimension"


class NodeMode(str, enum.Enum):
    """
    Node mode.

    A node can be in one of the following modes:

    1. PUBLISHED - Must be valid and not cause any child nodes to be invalid
    2. DRAFT - Can be invalid, have invalid parents, and include dangling references
    """

    PUBLISHED = "published"
    DRAFT = "draft"


class NodeStatus(str, enum.Enum):
    """
    Node status.

    A node can have one of the following statuses:

    1. VALID - All references to other nodes and node columns are valid
    2. INVALID - One or more parent nodes are incompatible or do not exist
    """

    VALID = "valid"
    INVALID = "invalid"


class NodeYAML(TypedDict, total=False):
    """
    Schema of a node in the YAML file.
    """

    description: str
    type: NodeType
    query: str
    columns: Dict[str, ColumnYAML]
    tables: Dict[str, List[TableYAML]]


class NodeBase(SQLModel):
    """
    A base node.
    """

    name: str = Field(sa_column=SqlaColumn("name", String, unique=True))
    description: str = ""
    type: NodeType = Field(sa_column=SqlaColumn(Enum(NodeType)))
    query: Optional[str] = None
    mode: NodeMode = NodeMode.PUBLISHED


class Node(NodeBase, table=True):  # type: ignore
    """
    A node.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    status: NodeStatus = NodeStatus.INVALID
    created_at: datetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )
    updated_at: datetime = Field(
        sa_column=SqlaColumn(DateTime(timezone=True)),
        default_factory=partial(datetime.now, timezone.utc),
    )

    tables: List[Table] = Relationship(
        back_populates="node",
        sa_relationship_kwargs={"cascade": "all, delete"},
    )

    parents: List["Node"] = Relationship(
        back_populates="children",
        link_model=NodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "Node.id==NodeRelationship.child_id",
            "secondaryjoin": "Node.id==NodeRelationship.parent_id",
        },
    )

    children: List["Node"] = Relationship(
        back_populates="parents",
        link_model=NodeRelationship,
        sa_relationship_kwargs={
            "primaryjoin": "Node.id==NodeRelationship.parent_id",
            "secondaryjoin": "Node.id==NodeRelationship.child_id",
        },
    )

    columns: List[Column] = Relationship(
        link_model=NodeColumns,
        sa_relationship_kwargs={
            "primaryjoin": "Node.id==NodeColumns.node_id",
            "secondaryjoin": "Column.id==NodeColumns.column_id",
            "cascade": "all, delete",
        },
    )

    def to_yaml(self) -> NodeYAML:
        """
        Serialize the node for YAML.

        This is used to update the original configuration with information about columns.
        """
        tables = defaultdict(list)
        for table in self.tables:  # pylint: disable=not-an-iterable
            tables[table.database.name].append(table.to_yaml())

        data = {
            "description": self.description,
            "type": self.type.value,  # pylint: disable=no-member
            "query": self.query,
            "columns": {
                column.name: column.to_yaml()
                for column in self.columns  # pylint: disable=not-an-iterable
            },
            "tables": dict(tables),
        }
        filtered = {key: value for key, value in data.items() if value}

        return cast(NodeYAML, filtered)

    def __hash__(self) -> int:
        return hash(self.id)

    def extra_validation(self) -> None:
        """
        Extra validation for node data.
        """
        if self.type == NodeType.SOURCE:
            if self.query:
                raise Exception(
                    f"Node {self.name} of type source should not have a query",
                )
            if not self.tables:
                raise Exception(
                    f"Node {self.name} of type source needs at least one table",
                )

        if self.type in {NodeType.TRANSFORM, NodeType.METRIC, NodeType.DIMENSION}:
            if not self.query:
                raise Exception(
                    f"Node {self.name} of type {self.type} needs a query",
                )

        if self.type == NodeType.METRIC:
            if not is_metric(self.query):
                raise Exception(
                    f"Node {self.name} of type metric has an invalid query, "
                    "should have a single aggregation",
                )
