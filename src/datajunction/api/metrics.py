"""
Metric related APIs.
"""

from datetime import datetime
from typing import Any, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from sqlmodel import Session, SQLModel, select

from datajunction.api.queries import save_query_and_run
from datajunction.config import Settings
from datajunction.models.node import Node
from datajunction.models.query import QueryWithResults
from datajunction.sql.parse import is_metric
from datajunction.sql.transpile import get_query_for_node
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


@router.get("/metrics/", response_model=List[Metric])
def read_metrics(*, session: Session = Depends(get_session)) -> List[Any]:
    """
    List all available metrics.
    """
    return [
        Metric(
            **node.dict(),
            dimensions=[
                f"{parent.name}/{column.name}"
                for parent in node.parents
                for column in parent.columns
            ],
        )
        for node in session.exec(select(Node))
        if node.expression and is_metric(node.expression)
    ]


@router.get("/metrics/{node_id}/data/", response_model=QueryWithResults)
def read_metrics_data(
    node_id: int,
    d: str = "",  # pylint: disable=invalid-name
    *,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
    response: Response,
    background_tasks: BackgroundTasks,
) -> QueryWithResults:
    """
    Return data for a metric.
    """
    node = session.get(Node, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Metric node not found")
    if not node.expression or not is_metric(node.expression):
        raise HTTPException(status_code=400, detail="Not a metric node")

    groupbys = d.split(",") if d else []
    create_query = get_query_for_node(node, groupbys)

    return save_query_and_run(
        create_query,
        session,
        settings,
        response,
        background_tasks,
    )
