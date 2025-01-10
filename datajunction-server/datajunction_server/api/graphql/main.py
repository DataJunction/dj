"""DJ graphql"""
from typing import Annotated, List, Optional, OrderedDict

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info

from datajunction_server.api.graphql.catalogs import CatalogInfo, list_catalogs
from datajunction_server.api.graphql.engines import EngineInfo, list_engines
from datajunction_server.api.graphql.resolvers.nodes import find_nodes_by
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.node import DimensionAttribute, Node
from datajunction_server.api.graphql.scalars.sql import GeneratedSQL
from datajunction_server.construction.build_v2 import get_measures_query
from datajunction_server.models.node import NodeCursor, NodeType
from datajunction_server.sql.dag import get_common_dimensions
from datajunction_server.utils import SEPARATOR, get_session, get_settings


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

    @strawberry.field(
        description="Get common dimensions for one or more nodes",
    )
    async def common_dimensions(
        self,
        nodes: Annotated[
            Optional[List[str]],
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
        nodes = await find_nodes_by(info, nodes)  # type: ignore
        dimensions = await get_common_dimensions(info.context["session"], nodes)  # type: ignore
        return [
            DimensionAttribute(  # type: ignore
                name=dim.name,
                attribute=dim.name.split(SEPARATOR)[-1],
                properties=dim.properties,
                type=dim.type,
            )
            for dim in dimensions
        ]

    @strawberry.field(
        description="Get measures SQL for a set of metrics with dimensions and filters",
    )
    async def measures_sql(
        self,
        metrics: Annotated[
            list[str] | None,
            strawberry.argument(
                description="A list of metric node names",
            ),
        ] = None,
        dimensions: Annotated[
            list[str] | None,
            strawberry.argument(
                description="A list of dimension attribute names",
            ),
        ] = None,
        filters: Annotated[
            list[str] | None,
            strawberry.argument(
                description="A list of filters names",
            ),
        ] = None,
        orderby: Annotated[
            list[str] | None,
            strawberry.argument(
                description="A list of order by clauses",
            ),
        ] = None,
        engine_name: Annotated[
            str | None,
            strawberry.argument(
                description="The name of the engine used by the generated SQL",
            ),
        ] = None,
        engine_version: Annotated[
            str | None,
            strawberry.argument(
                description="The version of the engine used by the generated SQL",
            ),
        ] = None,
        use_materialized: Annotated[
            bool,
            strawberry.argument(
                description="Whether to use materialized nodes where applicable",
            ),
        ] = True,
        include_all_columns: Annotated[
            bool,
            strawberry.argument(
                description="Whether to include all columns or only those necessary "
                "for the metrics and dimensions in the cube",
            ),
        ] = False,
        preaggregate: Annotated[
            bool,
            strawberry.argument(
                description="Whether to pre-aggregate to the requested dimensions so that "
                "subsequent queries are more efficient.",
            ),
        ] = False,
        *,
        info: Info,
    ) -> list[GeneratedSQL]:
        """
        Return a list of common dimensions for a set of metrics.
        """
        session, settings = info.context["session"], info.context["settings"]
        metrics = list(OrderedDict.fromkeys(metrics))  # type: ignore
        queries = await get_measures_query(
            session=session,
            metrics=metrics,
            dimensions=dimensions,  # type: ignore
            filters=filters,  # type: ignore
            orderby=orderby,
            engine_name=engine_name,
            engine_version=engine_version,
            include_all_columns=include_all_columns,
            sql_transpilation_library=settings.sql_transpilation_library,
            use_materialized=use_materialized,
            preaggregate=preaggregate,
        )
        return [
            await GeneratedSQL.from_pydantic(info, measures_query)
            for measures_query in queries
        ]


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
