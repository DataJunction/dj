"""
GQL Metric models and related APIs.
"""

# pylint: disable=too-few-public-methods, no-member

import datetime
from typing import List, Optional

import strawberry
from fastapi import HTTPException
from sqlmodel import select
from strawberry.types import Info

from dj.api.graphql.query import QueryWithResults
from dj.api.helpers import get_query
from dj.api.metrics import Metric as Metric_
from dj.api.metrics import TranslatedSQL as TranslatedSQL_
from dj.api.metrics import get_metric
from dj.api.queries import save_query_and_run
from dj.models.node import Node as Node_
from dj.models.node import NodeType as Node_Type
from dj.models.query import QueryCreate
from dj.sql.build import get_query_for_node


@strawberry.experimental.pydantic.type(
    model=Metric_,
    fields=[
        "id",
        "name",
        "display_name",
        "description",
        "query",
        "dimensions",
    ],
)
class Metric:
    """
    Class for a metric.
    """

    created_at: datetime.datetime
    updated_at: datetime.datetime


@strawberry.experimental.pydantic.type(model=TranslatedSQL_, all_fields=True)
class TranslatedSQL:
    """
    Class for SQL generated from a given metric.
    """


def read_metrics(info: Info) -> List[Metric]:
    """
    List all available metrics.
    """
    session = info.context["session"]
    return [
        Metric.from_pydantic(  # type: ignore
            Metric_.parse_node(node),
        )
        for node in session.exec(
            select(Node_).where(Node_.type == Node_Type.METRIC),
        )
    ]


def read_metric(node_name: str, info: Info) -> Metric:
    """
    Return a metric by name.
    """
    try:
        node = get_metric(info.context["session"], node_name)
    except HTTPException as exc:
        raise Exception(exc.detail) from exc

    return Metric.from_pydantic(  # type: ignore
        Metric_.parse_node(node),
    )


async def read_metrics_data(
    node_name: str,
    info: Info,
    database_name: Optional[str] = None,
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
) -> QueryWithResults:
    """
    Return data for a metric.
    """
    dimensions = dimensions or []
    filters = filters or []
    session = info.context["session"]
    query_ast, optimal_database = await get_query(
        session=session,
        metric=node_name,
        dimensions=dimensions,
        filters=filters,
        database_name=database_name,
    )
    create_query = QueryCreate(
        submitted_query=str(query_ast),
        database_id=optimal_database.id,
    )
    query_with_results = save_query_and_run(
        create_query,
        session,
        info.context["settings"],
        info.context["response"],
        info.context["background_tasks"],
    )

    return QueryWithResults.from_pydantic(query_with_results)  # type: ignore


async def read_metrics_sql(
    node_name: str,
    info: Info,
    database_name: Optional[str] = None,
    d: Optional[List[str]] = None,  # pylint: disable=invalid-name
    f: Optional[List[str]] = None,  # pylint: disable=invalid-name
) -> TranslatedSQL:
    """
    Return SQL for a metric.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    d = d or []
    f = f or []
    session = info.context["session"]
    try:
        node = get_metric(session, node_name)
    except HTTPException as ex:
        raise Exception(ex.detail) from ex
    create_query = await get_query_for_node(session, node, d, f, database_name)

    return TranslatedSQL.from_pydantic(  # type: ignore
        TranslatedSQL_(
            database_id=create_query.database_id,
            sql=create_query.submitted_query,
        ),
    )
