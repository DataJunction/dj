"""
DAG-related queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars.node import DimensionAttribute
from datajunction_server.sql.dag import get_common_dimensions
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
            properties=dim.properties,
            type=dim.type,
        )
        for dim in dimensions
    ]
