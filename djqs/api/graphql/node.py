"""
GQL Node models and related APIs.
"""

# pylint: disable=too-few-public-methods, no-member

from typing import TYPE_CHECKING, List

import strawberry
from sqlmodel import select
from strawberry.types import Info
from typing_extensions import Annotated

from djqs.models.node import Node as Node_
from djqs.models.node import NodeColumns as NodeColumns_
from djqs.models.node import NodeRelationship as Node_Relationship
from djqs.models.node import NodeType as NodeType_


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
    from djqs.api.graphql.column import Column
    from djqs.api.graphql.table import Table


@strawberry.experimental.pydantic.type(
    model=Node_,
    fields=["id", "name", "description", "created_at", "updated_at", "query"],
)
class Node:  # type: ignore
    """
    A node.
    """

    type: NodeType  # type: ignore
    tables: List[Annotated["Table", strawberry.lazy("djqs.api.graphql.table")]]
    parents: List[Annotated["Node", strawberry.lazy("djqs.api.graphql.node")]]
    children: List[Annotated["Node", strawberry.lazy("djqs.api.graphql.node")]]
    columns: List[Annotated["Column", strawberry.lazy("djqs.api.graphql.column")]]


def get_nodes(info: Info) -> List[Node]:
    """
    List the available nodes.
    """
    nodes = info.context["session"].exec(select(Node_)).all()
    return [Node.from_pydantic(node) for node in nodes]  # type: ignore
