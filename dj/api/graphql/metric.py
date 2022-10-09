"""
GQL Metric models and related APIs.
"""

from typing import List, Optional

import strawberry
from fastapi import HTTPException
from sqlmodel import select
from strawberry.types import Info

from dj.api.graphql.query import QueryWithResults
from dj.api.metrics import Metric as _Metric
from dj.api.metrics import TranslatedSQL as _TranslatedSQL
from dj.api.queries import save_query_and_run
from dj.config import Settings
from dj.models.node import Node as _Node
from dj.models.node import NodeType as _NodeType
from dj.sql.build import get_query_for_node
from dj.sql.dag import get_dimensions


@strawberry.experimental.pydantic.type(model=_Metric, all_fields=True)
class Metric:
    """
    Class for a metric.
    """


@strawberry.experimental.pydantic.type(model=_TranslatedSQL, all_fields=True)
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
        Metric.from_pydantic(_Metric(**node.dict(), dimensions=get_dimensions(node)))
        for node in session.exec(select(_Node).where(_Node.type == _NodeType.METRIC))
    ]


def read_metric(node_id: int, info: Info) -> Metric:
    """
    Return a metric by ID.
    """
    node = info.context["session"].get(_Node, node_id)
    if not node:
        raise Exception(
            "Metric node not found",
        )
    if node.type != _NodeType.METRIC:
        raise Exception(
            "Not a metric node",
        )
    return Metric.from_pydantic(_Metric(**node.dict(), dimensions=get_dimensions(node)))


from dj.api.metrics import get_metric


async def read_metrics_data(
    node_id: int,
    database_id: Optional[int] = None,
    d: Optional[List[str]] = None,  # pylint: disable=invalid-name
    f: Optional[List[str]] = None,  # pylint: disable=invalid-name
    info: Info = None,  # type: ignore
) -> QueryWithResults:
    """
    Return data for a metric.
    """
    d = d or []
    f = f or []
    session = info.context["session"]
    try:
        node = get_metric(session, node_id)
    except HTTPException as e:
        raise Exception(e.detail)
    create_query = await get_query_for_node(session, node, d, f, database_id)
    query_with_results = save_query_and_run(
        create_query,
        session,
        info.context["settings"],
        info.context["response"],
        info.context["background_tasks"],
    )

    return QueryWithResults.from_pydantic(query_with_results)


async def read_metrics_sql(
    node_id: int,
    database_id: Optional[int] = None,
    d: Optional[List[str]] = None,  # pylint: disable=invalid-name
    f: Optional[List[str]] = None,  # pylint: disable=invalid-name
    info: Info = None,  # type: ignore
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
    except HTTPException as e:
        raise Exception(e.detail)
    create_query = await get_query_for_node(session, node, d, f, database_id)

    return TranslatedSQL.from_pydantic(
        _TranslatedSQL(
            database_id=create_query.database_id,
            sql=create_query.submitted_query,
        ),
    )
