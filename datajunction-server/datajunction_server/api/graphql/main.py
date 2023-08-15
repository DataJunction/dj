from typing import List

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter

from datajunction_server.api.graphql.catalogs import list_catalogs, CatalogInfo
from datajunction_server.api.graphql.engines import list_engines, EngineInfo

from datajunction_server.utils import get_session, get_settings


async def get_context(session=Depends(get_session), settings=Depends(get_settings)):
    return {"session": session, "settings": settings}


@strawberry.type
class Query:
    list_catalogs: List[CatalogInfo] = strawberry.field(resolver=list_catalogs)
    list_engines: List[EngineInfo] = strawberry.field(resolver=list_engines)

schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema, context_getter=get_context)