"""DJ graphql"""
import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter

from datajunction_server.api.graphql.queries.catalogs import list_catalogs
from datajunction_server.api.graphql.queries.dag import common_dimensions
from datajunction_server.api.graphql.queries.engines import list_engines
from datajunction_server.api.graphql.queries.nodes import (
    find_nodes,
    find_nodes_paginated,
)
from datajunction_server.api.graphql.queries.sql import measures_sql
from datajunction_server.api.graphql.queries.tags import list_tag_types, list_tags
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.catalog_engine import Catalog, Engine
from datajunction_server.api.graphql.scalars.node import DimensionAttribute, Node
from datajunction_server.api.graphql.scalars.sql import GeneratedSQL
from datajunction_server.api.graphql.scalars.tag import Tag
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

    # Catalog and engine queries
    list_catalogs: list[Catalog] = strawberry.field(  # noqa: F811
        resolver=list_catalogs,
    )
    list_engines: list[Engine] = strawberry.field(  # noqa: F811
        resolver=list_engines,
    )

    # Node search queries
    find_nodes: list[Node] = strawberry.field(  # noqa: F811
        resolver=find_nodes,
        description="Find nodes based on the search parameters.",
    )
    find_nodes_paginated: Connection[Node] = strawberry.field(  # noqa: F811
        resolver=find_nodes_paginated,
        description="Find nodes based on the search parameters with pagination",
    )

    # DAG queries
    common_dimensions: list[DimensionAttribute] = strawberry.field(  # noqa: F811
        resolver=common_dimensions,
        description="Get common dimensions for one or more nodes",
    )

    # Generate SQL queries
    measures_sql: list[GeneratedSQL] = strawberry.field(  # noqa: F811
        resolver=measures_sql,
    )

    # Tags queries
    list_tags: list[Tag] = strawberry.field(  # noqa: F811
        resolver=list_tags,
        description="Find DJ node tags based on the search parameters.",
    )
    list_tag_types: list[str] = strawberry.field(  # noqa: F811
        resolver=list_tag_types,
        description="List all DJ node tag types",
    )


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
