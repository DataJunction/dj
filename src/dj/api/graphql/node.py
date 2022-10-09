"""
GQL Node models and related APIs.
"""

from typing import TYPE_CHECKING, List

import strawberry
from sqlmodel import select
from strawberry.types import Info
from typing_extensions import Annotated

from dj.models.node import Node as _Node
from dj.models.node import NodeColumns as _NodeColumns
from dj.models.node import NodeRelationship as _NodeRelationship
from dj.models.node import NodeType as _NodeType


@strawberry.experimental.pydantic.type(model=_NodeRelationship, all_fields=True)
class NodeRelationship:
    """
    Join table for self-referential many-to-many relationships between nodes.
    """


@strawberry.experimental.pydantic.type(model=_NodeColumns, all_fields=True)
class NodeColumns:
    """
    Join table for node columns.
    """


NodeType = strawberry.enum(_NodeType)

if TYPE_CHECKING:
    from dj.api.graphql.table import Table
    from dj.api.graphql.column import Column


@strawberry.experimental.pydantic.type(
    model=_Node,
    fields=["id", "name", "description", "created_at", "updated_at", "query"],
)
class Node:  # type: ignore
    """
    A node.
    """

    type: NodeType
    tables: List[Annotated["Table", strawberry.lazy("dj.api.graphql.table")]]
    parents: List[Annotated["Node", strawberry.lazy("dj.api.graphql.node")]]
    children: List[Annotated["Node", strawberry.lazy("dj.api.graphql.node")]]
    columns: List[
        Annotated["Column", strawberry.lazy("dj.api.graphql.column")]
    ]


def get_nodes(info: Info) -> List[Node]:
    """
    List the available nodes.
    """
    nodes = info.context["session"].exec(select(_Node)).all()
    return [Node.from_pydantic(node) for node in nodes]
