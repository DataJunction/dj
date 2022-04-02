"""
Metric related APIs.
"""

from typing import Any, List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Response
from sqlmodel import Session, select

from datajunction.api.queries import save_query_and_run
from datajunction.config import Settings
from datajunction.models.node import Node
from datajunction.models.query import QueryWithResults
from datajunction.sql.parse import is_metric
from datajunction.sql.transpile import get_query_for_node
from datajunction.utils import get_session, get_settings

router = APIRouter()


@router.get("/metrics/", response_model=List[Node])
def read_metrics(*, session: Session = Depends(get_session)) -> List[Any]:
    """
    List all available metrics.
    """
    nodes = [
        node
        for node in session.exec(select(Node))
        if node.expression and is_metric(node.expression)
    ]
    return nodes


@router.get("/metrics/{node_id}/data/", response_model=QueryWithResults)
def read_metrics_data(
    node_id: int,
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

    create_query = get_query_for_node(node)
    return save_query_and_run(
        create_query,
        session,
        settings,
        response,
        background_tasks,
    )
