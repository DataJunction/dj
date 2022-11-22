"""
Main GraphQL router.
"""

# pylint: disable=too-few-public-methods

from typing import List

import strawberry
from fastapi import Depends
from strawberry.fastapi import GraphQLRouter

from dj.api.graphql.database import Database
from dj.api.graphql.database import get_databases as get_databases_
from dj.api.graphql.metric import Metric, TranslatedSQL
from dj.api.graphql.metric import read_metric as read_metric_
from dj.api.graphql.metric import read_metrics as read_metrics_
from dj.api.graphql.metric import read_metrics_data as read_metrics_data_
from dj.api.graphql.metric import read_metrics_sql as read_metrics_sql_
from dj.api.graphql.node import Node
from dj.api.graphql.node import get_nodes as get_nodes_
from dj.api.graphql.query import QueryWithResults
from dj.api.graphql.query import submit_query as submit_query_
from dj.utils import get_session, get_settings


async def get_context(session=Depends(get_session), settings=Depends(get_settings)):
    """
    GQL context.
    """
    return {"session": session, "settings": settings}


@strawberry.type
class Query:
    """
    GQL query.
    """

    get_databases: List[Database] = strawberry.field(resolver=get_databases_)
    get_nodes: List[Node] = strawberry.field(resolver=get_nodes_)
    read_metrics: List[Metric] = strawberry.field(resolver=read_metrics_)
    read_metric: Metric = strawberry.field(resolver=read_metric_)
    read_metrics_data: QueryWithResults = strawberry.field(resolver=read_metrics_data_)
    read_metrics_sql: TranslatedSQL = strawberry.field(resolver=read_metrics_sql_)


@strawberry.type
class Mutation:
    """
    GQL mutation for submitting query.
    """

    submit_query: QueryWithResults = strawberry.mutation(resolver=submit_query_)


schema = strawberry.Schema(query=Query, mutation=Mutation)

graphql_app = GraphQLRouter(schema, context_getter=get_context)
graphql_app.description = "GraphQL endpoint"
