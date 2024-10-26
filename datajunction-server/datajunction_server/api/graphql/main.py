"""DJ graphql"""
from typing import Annotated, List, Optional

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from datajunction_server.api.graphql.catalogs import CatalogInfo, list_catalogs
from datajunction_server.api.graphql.engines import EngineInfo, list_engines
from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.node import Node
from datajunction_server.models.node import NodeCursor, NodeType
from datajunction_server.utils import get_session, get_settings


async def get_context(
    session=Depends(get_session),
    settings=Depends(get_settings),
):
    """
    Provides the context for graphql requests
    """
    return {"session": session, "settings": settings}


@strawberry.type
class Query:  # pylint: disable=R0903
    """
    Parent of all DJ graphql queries
    """

    list_catalogs: List[CatalogInfo] = strawberry.field(  # noqa: F811
        resolver=list_catalogs,
    )
    list_engines: List[EngineInfo] = strawberry.field(  # noqa: F811
        resolver=list_engines,
    )

    @strawberry.field(description="Find nodes based on the search parameters.")
    async def find_nodes(
        self,
        fragment: Annotated[
            Optional[str],
            strawberry.argument(
                description="A fragment of a node name to search for",
            ),
        ] = None,
        names: Annotated[
            Optional[List[str]],
            strawberry.argument(
                description="Filter to nodes with these names",
            ),
        ] = None,
        node_types: Annotated[
            Optional[List[NodeType]],
            strawberry.argument(
                description="Filter nodes to these node types",
            ),
        ] = None,
        tags: Annotated[
            Optional[List[str]],
            strawberry.argument(
                description="Filter to nodes tagged with these tags",
            ),
        ] = None,
        *,
        info: Info,
    ) -> List[Node]:
        """
        Find nodes based on the search parameters.
        """
        return await find_nodes_by(info, names, fragment, node_types, tags)  # type: ignore

    @strawberry.field(
        description="Find nodes based on the search parameters with pagination",
    )
    async def find_nodes_paginated(
        self,
        fragment: Annotated[
            Optional[str],
            strawberry.argument(
                description="A fragment of a node name to search for",
            ),
        ] = None,
        names: Annotated[
            Optional[List[str]],
            strawberry.argument(
                description="Filter to nodes with these names",
            ),
        ] = None,
        node_types: Annotated[
            Optional[List[NodeType]],
            strawberry.argument(
                description="Filter nodes to these node types",
            ),
        ] = None,
        tags: Annotated[
            Optional[List[str]],
            strawberry.argument(
                description="Filter to nodes tagged with these tags",
            ),
        ] = None,
        edited_by: Annotated[
            Optional[str],
            strawberry.argument(
                description="Filter to nodes edited by this user",
            ),
        ] = None,
        namespace: Annotated[
            Optional[str],
            strawberry.argument(
                description="Filter to nodes in this namespace",
            ),
        ] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        limit: Annotated[
            Optional[int],
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


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
