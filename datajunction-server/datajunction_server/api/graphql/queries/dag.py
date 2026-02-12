"""
DAG-related queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.database.node import Node
from datajunction_server.api.graphql.resolvers.nodes import (
    find_nodes_by,
    load_node_options,
)
from datajunction_server.api.graphql.scalars.node import DimensionAttribute
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.sql.dag import (
    get_common_dimensions,
    get_downstream_nodes,
    get_upstream_nodes,
)
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
    from sqlalchemy.orm import joinedload
    from datajunction_server.database.node import Node as DBNode
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name
    from datajunction_server.api.graphql.utils import extract_fields
    from datajunction_server.utils import session_context

    # Use a fresh session to avoid concurrent access issues with the shared context session
    async with session_context(info.context["request"]) as session:
        # Manually call DBNode.find_by with explicit eager loading of 'current' relationship
        # This prevents concurrent session access errors when get_common_dimensions
        # needs to access node.current later
        nodes_list = await DBNode.find_by(
            session,
            names=nodes,
            options=[joinedload(DBNode.current)],
        )

        dimensions = await get_common_dimensions(session, nodes_list)  # type: ignore

        # Eagerly load dimension nodes to prevent concurrent session access
        # when the dimensionNode field resolver is called
        fields = extract_fields(info)
        dimension_node_fields = fields.get("dimensionNode") if "dimensionNode" in fields else None

        result = []
        for dim in dimensions:
            dim_attr = DimensionAttribute(  # type: ignore
                name=dim.name,
                attribute=dim.name.split(SEPARATOR)[-1],
                properties=dim.properties,  # type: ignore
                type=dim.type,  # type: ignore
            )

            # If dimensionNode field is requested, pre-load it to avoid concurrent queries
            if dimension_node_fields:
                dimension_node_name = dim.name.rsplit(".", 1)[0]
                dim_attr._dimension_node = await get_node_by_name(
                    session=session,
                    fields=dimension_node_fields,
                    name=dimension_node_name,
                )

            result.append(dim_attr)

        return result


async def downstream_nodes(
    node_names: Annotated[
        list[str],
        strawberry.argument(
            description="The node names to find downstream nodes for.",
        ),
    ],
    node_type: Annotated[
        NodeType | None,
        strawberry.argument(
            description="The node type to filter the downstream nodes on.",
        ),
    ] = None,
    include_deactivated: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include deactivated nodes in the result.",
        ),
    ] = False,
    *,
    info: Info,
) -> list[Node]:
    """
    Return a list of downstream nodes for one or more nodes.
    Results are deduplicated by node ID.

    Note: Unlike upstreams, downstreams uses per-node queries because the
    fanout threshold check and BFS fallback work better with single nodes.
    """
    session = info.context["session"]

    # Build load options based on requested GraphQL fields
    fields = extract_fields(info)
    options = load_node_options(fields)

    all_downstreams: dict[int, Node] = {}
    for node_name in node_names:
        downstreams = await get_downstream_nodes(
            session,
            node_name=node_name,
            node_type=node_type,
            include_deactivated=include_deactivated,
            options=options,
        )
        for node in downstreams:
            if node.id not in all_downstreams:  # pragma: no cover
                all_downstreams[node.id] = node
    return list(all_downstreams.values())


async def upstream_nodes(
    node_names: Annotated[
        list[str],
        strawberry.argument(
            description="The node names to find upstream nodes for.",
        ),
    ],
    node_type: Annotated[
        NodeType | None,
        strawberry.argument(
            description="The node type to filter the upstream nodes on.",
        ),
    ] = None,
    include_deactivated: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include deactivated nodes in the result.",
        ),
    ] = False,
    *,
    info: Info,
) -> list[Node]:
    """
    Return a list of upstream nodes for one or more nodes.
    Results are deduplicated by node ID.
    """
    session = info.context["session"]

    # Build load options based on requested GraphQL fields
    fields = extract_fields(info)
    options = load_node_options(fields)

    return await get_upstream_nodes(  # type: ignore
        session,
        node_name=node_names,
        node_type=node_type,
        include_deactivated=include_deactivated,
        options=options,
    )
