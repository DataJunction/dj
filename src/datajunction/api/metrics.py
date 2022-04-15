"""
Metric related APIs.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response
from sqlmodel import Session, SQLModel, select

from datajunction.api.queries import save_query_and_run
from datajunction.config import Settings
from datajunction.models.node import Node, NodeType
from datajunction.models.query import QueryWithResults
from datajunction.sql.build import get_query_for_node
from datajunction.utils import get_session, get_settings

router = APIRouter()


class Metric(SQLModel):
    """
    Class for a metric.
    """

    id: int
    name: str
    description: str = ""

    created_at: datetime
    updated_at: datetime

    expression: str

    dimensions: List[str]


class TranslatedSQL(SQLModel):
    """
    Class for SQL generated from a given metric.
    """

    database_id: int
    sql: str


@router.get("/metrics/", response_model=List[Metric])
def read_metrics(*, session: Session = Depends(get_session)) -> List[Metric]:
    """
    List all available metrics.
    """
    return [
        Metric(
            **node.dict(),
            dimensions=[
                f"{parent.name}.{column.name}"
                for parent in node.parents
                for column in parent.columns
            ],
        )
        for node in session.exec(select(Node).where(Node.type == NodeType.METRIC))
    ]


@router.get("/metrics/{node_id}/", response_model=Metric)
def read_metric(node_id: int, *, session: Session = Depends(get_session)) -> Metric:
    """
    Return a metric by ID.
    """
    node = session.get(Node, node_id)
    return Metric(
        **node.dict(),
        dimensions=[
            f"{parent.name}.{column.name}"
            for parent in node.parents
            for column in parent.columns
        ],
    )


def get_metric(session: Session, node_id: int) -> Node:
    """
    Return a metric node given a node ID.
    """
    node = session.get(Node, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Metric node not found")
    if node.type != NodeType.METRIC:
        raise HTTPException(status_code=400, detail="Not a metric node")
    return node


@router.get("/metrics/{node_id}/data/", response_model=QueryWithResults)
def read_metrics_data(
    node_id: int,
    database_id: Optional[int] = None,
    d: List[str] = Query([]),  # pylint: disable=invalid-name
    f: List[str] = Query([]),  # pylint: disable=invalid-name
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    response: Response,
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Return data for a metric.
    """
    node = get_metric(session, node_id)
    create_query = get_query_for_node(node, d, f, database_id)

    return save_query_and_run(
        create_query,
        session,
        settings,
        response,
        background_tasks,
    )


@router.get("/metrics/{node_id}/sql/", response_model=TranslatedSQL)
def read_metrics_sql(
    node_id: int,
    database_id: Optional[int] = None,
    d: List[str] = Query([]),  # pylint: disable=invalid-name
    f: List[str] = Query([]),  # pylint: disable=invalid-name
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a metric.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    node = get_metric(session, node_id)
    create_query = get_query_for_node(node, d, f, database_id)

    return TranslatedSQL(
        database_id=create_query.database_id,
        sql=create_query.submitted_query,
    )
