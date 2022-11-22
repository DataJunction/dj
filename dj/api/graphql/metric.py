"""
GQL Metric models and related APIs.
"""

# pylint: disable=too-few-public-methods, no-member

from typing import List, Optional

import strawberry
from fastapi import HTTPException
from sqlmodel import select
from strawberry.types import Info

from dj.api.graphql.query import QueryWithResults
from dj.api.metrics import Metric as Metric_
from dj.api.metrics import TranslatedSQL as TranslatedSQL_
from dj.api.metrics import get_metric
from dj.api.queries import save_query_and_run
from dj.models.node import Node as Node_
from dj.models.node import NodeType as Node_Type
from dj.sql.build import get_query_for_node
from dj.sql.dag import get_dimensions


@strawberry.experimental.pydantic.type(model=Metric_, all_fields=True)
class Metric:
    """
    Class for a metric.
    """


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
            Metric_(**node.dict(), dimensions=get_dimensions(node)),
        )
        for node in session.exec(select(Node_).where(Node_.type == Node_Type.METRIC))
    ]


def read_metric(node_id: int, info: Info) -> Metric:
    """
    Return a metric by ID.
    """
    node = info.context["session"].get(Node_, node_id)
    if not node:
        raise Exception(
            "Metric node not found",
        )
    if node.type != Node_Type.METRIC:
        raise Exception(
            "Not a metric node",
        )
    return Metric.from_pydantic(  # type: ignore
        Metric_(**node.dict(), dimensions=get_dimensions(node)),
    )


async def read_metrics_data(
    node_id: int,
    info: Info,
    database_id: Optional[int] = None,
    d: Optional[List[str]] = None,  # pylint: disable=invalid-name
    f: Optional[List[str]] = None,  # pylint: disable=invalid-name
) -> QueryWithResults:
    """
    Return data for a metric.
    """
    d = d or []
    f = f or []
    session = info.context["session"]
    try:
        node = get_metric(session, node_id)
    except HTTPException as ex:
        raise Exception(ex.detail) from ex
    create_query = await get_query_for_node(session, node, d, f, database_id)
    query_with_results = save_query_and_run(
        create_query,
        session,
        info.context["settings"],
        info.context["response"],
        info.context["background_tasks"],
    )

    return QueryWithResults.from_pydantic(query_with_results)  # type: ignore


async def read_metrics_sql(
    node_id: int,
    info: Info,
    database_id: Optional[int] = None,
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
        node = get_metric(session, node_id)
    except HTTPException as ex:
        raise Exception(ex.detail) from ex
    create_query = await get_query_for_node(session, node, d, f, database_id)

    return TranslatedSQL.from_pydantic(  # type: ignore
        TranslatedSQL_(
            database_id=create_query.database_id,
            sql=create_query.submitted_query,
        ),
    )
