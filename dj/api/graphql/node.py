"""
GQL Node models and related APIs.
"""
# pylint: disable=too-few-public-methods, no-member

import datetime
from typing import TYPE_CHECKING, List

import strawberry
from sqlmodel import select
from strawberry.types import Info
from typing_extensions import Annotated

from dj.models.node import Node as Node_
from dj.models.node import NodeColumns as NodeColumns_
from dj.models.node import NodeRelationship as Node_Relationship
from dj.models.node import NodeRevision as NodeRevision_
from dj.models.node import NodeType as NodeType_


@strawberry.experimental.pydantic.type(model=Node_Relationship, all_fields=True)
class NodeRelationship:
    """
    Join table for self-referential many-to-many relationships between nodes.
    """


@strawberry.experimental.pydantic.type(model=NodeColumns_, all_fields=True)
class NodeColumns:
    """
    Join table for node columns.
    """


NodeType = strawberry.enum(NodeType_)

if TYPE_CHECKING:
    from dj.api.graphql.column import Column
    from dj.api.graphql.table import Table


@strawberry.experimental.pydantic.type(
    model=NodeRevision_,
    fields=["id", "description", "query", "version"],
)
class NodeRevision:  # type: ignore
    """
    A node.

    Nodes can store data in multiple tables, in different databases.
    """

    type: NodeType  # type: ignore
    tables: List[Annotated["Table", strawberry.lazy("dj.api.graphql.table")]]
    parents: List[Annotated["Node", strawberry.lazy("dj.api.graphql.node")]]
    children: List[Annotated["NodeRevision", strawberry.lazy("dj.api.graphql.node")]]
    columns: List[Annotated["Column", strawberry.lazy("dj.api.graphql.column")]]
    updated_at: datetime.datetime


@strawberry.experimental.pydantic.type(
    model=Node_,
    fields=["id", "name", "current_version"],
)
class Node:  # type: ignore
    """
    A node.
    """

    type: NodeType  # type: ignore
    current: Annotated["NodeRevision", strawberry.lazy("dj.api.graphql.node")]
    created_at: datetime.datetime


def get_nodes(info: Info) -> List[Node]:
    """
    List the available nodes.
    """
    nodes = info.context["session"].exec(select(Node_)).all()
    return [Node.from_pydantic(node) for node in nodes]  # type: ignore
