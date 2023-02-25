"""
Metric related APIs.
"""

from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select

from dj.api.helpers import get_node_by_name, get_query
from dj.models.metric import Metric, TranslatedSQL
from dj.models.node import Node, NodeType
from dj.utils import get_session

router = APIRouter()


def get_metric(session: Session, name: str) -> Node:
    """
    Return a metric node given a node name.
    """
    node = get_node_by_name(session, name)
    if node.type != NodeType.METRIC:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=f"Not a metric node: `{name}`",
        )
    return node


@router.get("/metrics/", response_model=List[Metric])
def read_metrics(*, session: Session = Depends(get_session)) -> List[Metric]:
    """
    List all available metrics.
    """
    return [
        Metric.parse_node(node)
        for node in (
            session.exec(
                select(Node).where(Node.type == NodeType.METRIC),
            )
        )
    ]


@router.get("/metrics/{name}/", response_model=Metric)
def read_metric(name: str, *, session: Session = Depends(get_session)) -> Metric:
    """
    Return a metric by name.
    """
    node = get_metric(session, name)
    return Metric.parse_node(node)


@router.get("/metrics/{name}/sql/", response_model=TranslatedSQL)
async def read_metrics_sql(
    name: str,
    dimensions: List[str] = Query([]),  # pylint: disable=invalid-name
    filters: List[str] = Query([]),  # pylint: disable=invalid-name
    database_name: Optional[str] = None,
    check_database_online: bool = True,
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a metric.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    query_ast, optimal_database = await get_query(
        session=session,
        metric=name,
        dimensions=dimensions,
        filters=filters,
        database_name=database_name,
        check_database_online=check_database_online,
    )
    return TranslatedSQL(
        database_id=optimal_database.id,
        sql=str(query_ast),
    )
