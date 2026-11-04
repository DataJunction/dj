"""
Physical-table lookup queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import (
    find_nodes_by,
    load_node_options,
)
from datajunction_server.api.graphql.utils import extract_fields, resolver_session
from datajunction_server.database.node import Node
from datajunction_server.models.node import NodeCursor
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import get_downstream_nodes

# ``find_by`` always applies a limit, so sources are read a page at a time.
SOURCE_PAGE_SIZE = 500


async def _all_sources(info: Info, table: str) -> list[Node]:
    """
    Every source node on ``table``, read page by page so none are dropped.
    """
    sources: list[Node] = []
    after = None
    while True:
        # One extra row: ``after`` is inclusive, so it doubles as the next cursor.
        page = await find_nodes_by(
            info,
            tables=[table],
            node_types=[NodeType.SOURCE],
            limit=SOURCE_PAGE_SIZE + 1,
            after=after,
        )
        sources.extend(page[:SOURCE_PAGE_SIZE])
        if len(page) <= SOURCE_PAGE_SIZE:
            return sources
        after = NodeCursor(
            created_at=page[SOURCE_PAGE_SIZE].created_at,
            id=page[SOURCE_PAGE_SIZE].id,
        ).encode()


async def nodes_for_table(
    table: Annotated[
        str,
        strawberry.argument(
            description="The physical table to look up. May be fully qualified "
            "(catalog.schema.table), partially qualified (schema.table) or a bare "
            "table name; matching is case-insensitive.",
        ),
    ],
    node_types: Annotated[
        list[NodeType] | None,
        strawberry.argument(
            description="Filter the returned nodes to these node types. Applies to "
            "the whole result, the source nodes included.",
        ),
    ] = None,
    include_downstream: Annotated[
        bool,
        strawberry.argument(
            description="Also return everything transitively downstream of the "
            "source nodes on this table (transforms, dimensions, metrics, cubes).",
        ),
    ] = True,
    include_deactivated: Annotated[
        bool,
        strawberry.argument(
            description="Whether to include deactivated downstream nodes.",
        ),
    ] = False,
    *,
    info: Info,
) -> list[Node]:
    """
    Return the DJ nodes associated with a physical table.

    A table only ever identifies source nodes -- ``table`` is a column on the
    source node's revision -- so the transforms, dimensions, metrics and cubes
    built on it are reached by walking the DAG down from those sources.
    Results are deduplicated by node ID.
    """
    sources = await _all_sources(info, table)

    found: dict[int, Node] = {source.id: source for source in sources}
    if include_downstream and sources:
        async with resolver_session(info) as session:
            options = load_node_options(extract_fields(info))
            for source in sources:
                downstreams = await get_downstream_nodes(
                    session,
                    node_name=source.name,
                    include_deactivated=include_deactivated,
                    options=options,
                )
                for node in downstreams:
                    found.setdefault(node.id, node)

    nodes = list(found.values())
    if node_types:
        wanted = {NodeType(node_type) for node_type in node_types}
        nodes = [node for node in nodes if NodeType(node.type) in wanted]
    return nodes
