"""DJ graphql"""

from typing import List

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter

from datajunction_server.api.graphql.catalogs import CatalogInfo, list_catalogs
from datajunction_server.api.graphql.engines import EngineInfo, list_engines
from datajunction_server.utils import get_session, get_settings


async def get_context(
    session=Depends(get_session),
    settings=Depends(get_settings),
):
    """
    provides the context for graphql requests
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


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
