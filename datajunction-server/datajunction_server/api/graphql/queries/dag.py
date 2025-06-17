"""
DAG-related queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.database.node import Node
from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars.node import DimensionAttribute
from datajunction_server.sql.dag import get_common_dimensions, get_downstream_nodes
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR


async def common_dimensions(
    nodes: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of nodes to find common dimensions for",
        ),
    ] = None,
    *,
    info: Info,
) -> list[DimensionAttribute]:
    """
    Return a list of common dimensions for a set of nodes.
    """
    nodes = await find_nodes_by(info, nodes)  # type: ignore
    dimensions = await get_common_dimensions(info.context["session"], nodes)  # type: ignore
    return [
        DimensionAttribute(  # type: ignore
            name=dim.name,
            attribute=dim.name.split(SEPARATOR)[-1],
            properties=dim.properties,  # type: ignore
            type=dim.type,  # type: ignore
        )
        for dim in dimensions
    ]


async def downstream_nodes(
    node_name: Annotated[
        str,
        strawberry.argument(
            description="The node name to find downstream nodes for.",
        ),
    ],
    node_type: Annotated[
        NodeType | None,
        strawberry.argument(
            description="The node type to filter the downstream nodes on.",
        ),
    ] = None,
    *,
    info: Info,
) -> list[Node]:
    """
    Return a list of downstream nodes for a given node.
    """
    session = info.context["session"]
    return await get_downstream_nodes(session, node_name=node_name, node_type=node_type)  # type: ignore
