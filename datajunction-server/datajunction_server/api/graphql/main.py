"""DJ graphql"""

import logging
from functools import wraps

import strawberry
from fastapi import Request
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from datajunction_server.api.graphql.queries.catalogs import list_catalogs
from datajunction_server.api.graphql.queries.dag import (
    common_dimensions,
    downstream_nodes,
)
from datajunction_server.api.graphql.queries.engines import list_engines, list_dialects
from datajunction_server.api.graphql.queries.nodes import (
    find_nodes,
    find_nodes_paginated,
)
from datajunction_server.api.graphql.queries.sql import measures_sql
from datajunction_server.api.graphql.queries.tags import list_tag_types, list_tags
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.catalog_engine import (
    Catalog,
    Engine,
    DialectInfo,
)
from datajunction_server.api.graphql.scalars.node import DimensionAttribute, Node
from datajunction_server.api.graphql.scalars.sql import GeneratedSQL
from datajunction_server.api.graphql.scalars.tag import Tag
from datajunction_server.utils import get_settings

logger = logging.getLogger(__name__)


def log_resolver(func):
    """
    Adds generic logging to the GQL resolver.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        resolver_name = func.__name__

        info: Info = kwargs.get("info") if "info" in kwargs else None
        user = info.context.get("user", "anonymous") if info else "unknown"
        args_dict = {key: val for key, val in kwargs.items() if key != "info"}
        log_tags = {
            "query_name": resolver_name,
            "user": user,
            **args_dict,
        }
        log_args = " ".join(
            [f"{tag}={value}" for tag, value in log_tags.items() if value],
        )
        try:
            result = await func(*args, **kwargs)
            logger.info("[GQL] %s", log_args)
            return result
        except Exception as exc:  # pragma: no cover
            logger.error(  # pragma: no cover
                "[GQL] status=error %s",
                log_args,
                exc_info=True,
            )
            raise exc  # pragma: no cover

    return wrapper


async def get_context(request: Request):
    """
    Provides the context for graphql requests
    """
    return {
        "session": request.state.db,
        "settings": get_settings(),
    }


@strawberry.type
class Query:
    """
    Parent of all DJ graphql queries
    """

    # Catalog and engine queries
    list_catalogs: list[Catalog] = strawberry.field(
        resolver=log_resolver(list_catalogs),
        description="List available catalogs",
    )
    list_engines: list[Engine] = strawberry.field(
        resolver=log_resolver(list_engines),
        description="List all available engines",
    )
    list_dialects: list[DialectInfo] = strawberry.field(
        resolver=log_resolver(list_dialects),
        description="List all supported SQL dialects",
    )

    # Node search queries
    find_nodes: list[Node] = strawberry.field(
        resolver=log_resolver(find_nodes),
        description="Find nodes based on the search parameters.",
    )
    find_nodes_paginated: Connection[Node] = strawberry.field(
        resolver=log_resolver(find_nodes_paginated),
        description="Find nodes based on the search parameters with pagination",
    )

    # DAG queries
    common_dimensions: list[DimensionAttribute] = strawberry.field(
        resolver=log_resolver(common_dimensions),
        description="Get common dimensions for one or more nodes",
    )
    downstream_nodes: list[Node] = strawberry.field(
        resolver=log_resolver(downstream_nodes),
        description="Find downstream nodes (optionally, of a given type) from a given node.",
    )

    # Generate SQL queries
    measures_sql: list[GeneratedSQL] = strawberry.field(
        resolver=log_resolver(measures_sql),
        description="Get measures SQL for a list of metrics, dimensions, and filters.",
    )

    # Tags queries
    list_tags: list[Tag] = strawberry.field(
        resolver=log_resolver(list_tags),
        description="Find DJ node tags based on the search parameters.",
    )
    list_tag_types: list[str] = strawberry.field(
        resolver=log_resolver(list_tag_types),
        description="List all DJ node tag types",
    )


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
