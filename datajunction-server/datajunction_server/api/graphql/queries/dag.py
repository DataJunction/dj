"""
DAG-related queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.database.node import Node
from datajunction_server.api.graphql.resolvers.nodes import load_node_options
from datajunction_server.api.graphql.scalars.node import DimensionAttribute
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.sql.dag import (
    get_common_dimensions,
    get_downstream_nodes,
    get_upstream_nodes,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR, session_context


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
    # Use DataLoader to batch load input nodes
    node_loader = info.context["node_loader"]
    nodes_list = await node_loader.load_many(nodes)

    # Filter out nodes that don't exist
    nodes_list = [node for node in nodes_list if node is not None]

    async with session_context(info.context["request"]) as session:
        dimensions = await get_common_dimensions(session, nodes_list)  # type: ignore

    result = [
        DimensionAttribute(  # type: ignore
            name=dim.name,
            attribute=dim.name.split(SEPARATOR)[-1],
            properties=dim.properties,  # type: ignore
            type=dim.type,  # type: ignore
        )
        for dim in dimensions
    ]

    # Check if dimensionNode field is requested
    fields = extract_fields(info)
    # Note: extract_fields converts camelCase to snake_case
    has_dimension_node_field = "dimension_node" in fields

    if has_dimension_node_field:
        dimension_node_names = list(
            {dim.name.rsplit(".", 1)[0] for dim in dimensions},
        )
        dimension_node_fields = fields.get("dimension_node", {})

        # Load dimension nodes with proper field selection using a fresh session
        # We can't use the simple DataLoader here because we need custom field loading
        async with session_context(info.context["request"]) as session:
            loaded_nodes = await Node.find_by(
                session,
                names=dimension_node_names,
                options=load_node_options(dimension_node_fields),
            )

        # Create lookup map
        node_map = {node.name: node for node in loaded_nodes}

        # Pre-populate the _dimension_node on each DimensionAttribute
        for dim_attr in result:
            dimension_node_name = dim_attr.name.rsplit(".", 1)[0]
            dim_attr._dimension_node = node_map.get(dimension_node_name)

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
