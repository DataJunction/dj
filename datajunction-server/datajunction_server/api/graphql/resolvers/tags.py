"""
Node resolvers
"""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.tag import Tag as DBTag


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
