"""
Find nodes GraphQL queries.
"""
from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.node import Node
from datajunction_server.models.node import NodeCursor, NodeType


async def find_nodes(
    fragment: Annotated[
        str | None,
        strawberry.argument(
            description="A fragment of a node name to search for",
        ),
    ] = None,
    names: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes with these names",
        ),
    ] = None,
    node_types: Annotated[
        list[NodeType] | None,
        strawberry.argument(
            description="Filter nodes to these node types",
        ),
    ] = None,
    tags: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes tagged with these tags",
        ),
    ] = None,
    *,
    info: Info,
) -> list[Node]:
    """
    Find nodes based on the search parameters.
    """
    return await find_nodes_by(info, names, fragment, node_types, tags)  # type: ignore


async def find_nodes_paginated(
    fragment: Annotated[
        str | None,
        strawberry.argument(
            description="A fragment of a node name to search for",
        ),
    ] = None,
    names: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes with these names",
        ),
    ] = None,
    node_types: Annotated[
        list[NodeType] | None,
        strawberry.argument(
            description="Filter nodes to these node types",
        ),
    ] = None,
    tags: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes tagged with these tags",
        ),
    ] = None,
    edited_by: Annotated[
        str | None,
        strawberry.argument(
            description="Filter to nodes edited by this user",
        ),
    ] = None,
    namespace: Annotated[
        str | None,
        strawberry.argument(
            description="Filter to nodes in this namespace",
        ),
    ] = None,
    after: str | None = None,
    before: str | None = None,
    limit: Annotated[
        int | None,
        strawberry.argument(description="Limit nodes"),
    ] = 100,
    *,
    info: Info,
) -> Connection[Node]:
    """
    Find nodes based on the search parameters.
    """
    if not limit or limit < 0:
        limit = 100
    nodes_list = await find_nodes_by(
        info,
        names,
        fragment,
        node_types,
        tags,
        edited_by,
        namespace,
        limit + 1,
        before,
        after,
    )
    return Connection.from_list(
        items=nodes_list,
        before=before,
        after=after,
        limit=limit,
        encode_cursor=lambda dj_node: NodeCursor(
            created_at=dj_node.created_at,
            id=dj_node.id,
        ),
    )
