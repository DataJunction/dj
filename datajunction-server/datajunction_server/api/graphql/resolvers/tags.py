"""
Tag resolvers
"""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only, noload

from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.tag import Tag as DBTag

# GraphQL TagBase fields → DB Tag columns. id is always included (PK).
_TAG_FIELD_TO_COLUMN = {
    "name": DBTag.name,
    "tag_type": DBTag.tag_type,
    "description": DBTag.description,
    "display_name": DBTag.display_name,
    "tag_metadata": DBTag.tag_metadata,
}


def tag_load_options(requested_fields=None):
    """
    Load options for Tag objects in GraphQL queries.

    Restricts columns to those actually requested and suppresses
    all relationships (created_by, nodes).
    """
    options = [
        noload(DBTag.created_by),
        noload(DBTag.nodes),
    ]
    if requested_fields:  # pragma: no branch
        cols = [DBTag.id] + [
            col
            for field, col in _TAG_FIELD_TO_COLUMN.items()
            if field in requested_fields
        ]
        options.append(load_only(*cols))
    return options


async def get_nodes_by_tag(
    session: AsyncSession,
    tag_name: str,
    fields: dict[str, Any],
) -> list[DBNode]:
    """
    Retrieves all nodes with the given tag. A list of fields must be requested on the node,
    or this will not return any data.
    """
    from datajunction_server.api.graphql.resolvers.nodes import load_node_options

    options = load_node_options(
        fields["nodes"]
        if "nodes" in fields
        else fields["edges"]["node"]
        if "edges" in fields
        else fields,
    )
    return await DBTag.list_nodes_with_tag(
        session,
        tag_name=tag_name,
        options=options,
    )
