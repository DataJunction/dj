"""
Find nodes GraphQL queries.
"""

import logging
from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import (
    count_nodes_by,
    find_nodes_by,
)
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.node import Node, NodeSortField
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.models.node import NodeCursor, NodeMode, NodeStatus, NodeType

DEFAULT_LIMIT = 1000
UPPER_LIMIT = 10000

logger = logging.getLogger(__name__)


async def find_nodes(
    search: Annotated[
        str | None,
        strawberry.argument(
            description=(
                "Full-text search across node name, display name, and description. "
                "Results are ranked by pg_trgm similarity and boosted toward the "
                "main branch when a node exists in a git-backed namespace."
            ),
        ),
    ] = None,
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
    dimensions: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes that have ALL of these dimensions. "
            "Accepts dimension node names or dimension attributes",
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
    mode: Annotated[
        NodeMode | None,
        strawberry.argument(
            description="Filter to nodes with this mode (published or draft)",
        ),
    ] = None,
    owned_by: Annotated[
        str | None,
        strawberry.argument(
            description="Filter to nodes owned by this user",
        ),
    ] = None,
    include_team: Annotated[
        bool,
        strawberry.argument(
            description="When combined with ownedBy, also include nodes owned "
            "by any group the user is a member of.",
        ),
    ] = False,
    missing_description: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes missing descriptions (for data quality checks)",
        ),
    ] = False,
    missing_owner: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes without any owners (for data quality checks)",
        ),
    ] = False,
    statuses: Annotated[
        list[NodeStatus] | None,
        strawberry.argument(
            description="Filter to nodes with these statuses (e.g., VALID, INVALID)",
        ),
    ] = None,
    has_materialization: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes that have materializations configured",
        ),
    ] = False,
    orphaned_dimension: Annotated[
        bool,
        strawberry.argument(
            description="Filter to dimension nodes that are not linked to by any other node",
        ),
    ] = False,
    limit: Annotated[
        int | None,
        strawberry.argument(description="Limit nodes"),
    ] = DEFAULT_LIMIT,
    order_by: NodeSortField = NodeSortField.CREATED_AT,
    ascending: bool = False,
    *,
    info: Info,
) -> list[Node]:
    """
    Find nodes based on the search parameters.
    """
    if not limit or limit < 0:
        limit = DEFAULT_LIMIT

    if limit > UPPER_LIMIT:
        logger.warning(
            "Limit of %s is greater than the maximum limit of %s. Setting limit to %s.",
            limit,
            UPPER_LIMIT,
            UPPER_LIMIT,
        )
        limit = UPPER_LIMIT

    return await find_nodes_by(  # type: ignore
        info=info,
        names=names,
        fragment=fragment,
        node_types=node_types,
        tags=tags,
        dimensions=dimensions,
        edited_by=edited_by,
        namespace=namespace,
        mode=mode,
        owned_by=owned_by,
        include_team=include_team,
        missing_description=missing_description,
        missing_owner=missing_owner,
        statuses=statuses,
        has_materialization=has_materialization,
        orphaned_dimension=orphaned_dimension,
        limit=limit,
        order_by=order_by,
        ascending=ascending,
        search=search,
    )


async def find_nodes_paginated(
    search: Annotated[
        str | None,
        strawberry.argument(
            description=(
                "Full-text search across node name, display name, and description. "
                "When set, results are filtered by trigram match. Ranking by "
                "similarity is applied only on the unpaginated `findNodes` query; "
                "paginated results keep the normal sort order."
            ),
        ),
    ] = None,
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
    dimensions: Annotated[
        list[str] | None,
        strawberry.argument(
            description="Filter to nodes that have ALL of these dimensions. "
            "Accepts dimension node names or dimension attributes",
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
    mode: Annotated[
        NodeMode | None,
        strawberry.argument(
            description="Filter to nodes with this mode (published or draft)",
        ),
    ] = None,
    owned_by: Annotated[
        str | None,
        strawberry.argument(
            description="Filter to nodes owned by this user",
        ),
    ] = None,
    include_team: Annotated[
        bool,
        strawberry.argument(
            description="When combined with ownedBy, also include nodes owned "
            "by any group the user is a member of.",
        ),
    ] = False,
    missing_description: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes missing descriptions (for data quality checks)",
        ),
    ] = False,
    missing_owner: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes without any owners (for data quality checks)",
        ),
    ] = False,
    statuses: Annotated[
        list[NodeStatus] | None,
        strawberry.argument(
            description="Filter to nodes with these statuses (e.g., VALID, INVALID)",
        ),
    ] = None,
    has_materialization: Annotated[
        bool,
        strawberry.argument(
            description="Filter to nodes that have materializations configured",
        ),
    ] = False,
    orphaned_dimension: Annotated[
        bool,
        strawberry.argument(
            description="Filter to dimension nodes that are not linked to by any other node",
        ),
    ] = False,
    after: str | None = None,
    before: str | None = None,
    limit: Annotated[
        int | None,
        strawberry.argument(description="Limit nodes"),
    ] = 100,
    order_by: NodeSortField = NodeSortField.CREATED_AT,
    ascending: bool = False,
    *,
    info: Info,
) -> Connection[Node]:
    """
    Find nodes based on the search parameters.
    """
    if not limit or limit < 0:
        limit = 100
    nodes_list = await find_nodes_by(
        info=info,
        names=names,
        fragment=fragment,
        node_types=node_types,
        tags=tags,
        dimensions=dimensions,
        edited_by=edited_by,
        namespace=namespace,
        limit=limit + 1,
        before=before,
        after=after,
        order_by=order_by,
        ascending=ascending,
        mode=mode,
        owned_by=owned_by,
        include_team=include_team,
        missing_description=missing_description,
        missing_owner=missing_owner,
        statuses=statuses,
        has_materialization=has_materialization,
        orphaned_dimension=orphaned_dimension,
        search=search,
    )

    # Run a separate count query only when the client asked for totalCount
    total_count: int | None = None
    if "total_count" in extract_fields(info):
        total_count = await count_nodes_by(
            info=info,
            names=names,
            fragment=fragment,
            node_types=node_types,
            tags=tags,
            dimensions=dimensions,
            edited_by=edited_by,
            namespace=namespace,
            mode=mode,
            owned_by=owned_by,
            include_team=include_team,
            missing_description=missing_description,
            missing_owner=missing_owner,
            statuses=statuses,
            has_materialization=has_materialization,
            orphaned_dimension=orphaned_dimension,
            search=search,
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
        total_count=total_count,
    )
