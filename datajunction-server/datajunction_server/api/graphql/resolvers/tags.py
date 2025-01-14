"""
Node resolvers
"""
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.graphql.scalars.node import NodeName
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.tag import Tag as DBTag


async def get_nodes_by_tag(
    session: AsyncSession,
    tag_name: str,
    fields: dict[str, Any] | None,
) -> list[DBNode | NodeName]:
    """
    Retrieves a node by name. This function also tries to optimize the database
    query by only retrieving joined-in fields if they were requested.
    """
    if not fields:
        return None
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
