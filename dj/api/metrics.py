"""
Metric related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from dj.api.helpers import get_node_by_name
from dj.errors import DJError, DJException, ErrorCode
from dj.models.metric import Metric
from dj.models.node import Node, NodeType
from dj.sql.dag import get_shared_dimensions
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
def list_metrics(*, session: Session = Depends(get_session)) -> List[Metric]:
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
def get_a_metric(name: str, *, session: Session = Depends(get_session)) -> Metric:
    """
    Return a metric by name.
    """
    node = get_metric(session, name)
    return Metric.parse_node(node)


@router.get("/metrics/common/dimensions/", response_model=List[str])
async def get_common_dimensions(
    metric: List[str] = Query(
        title="List of metrics to find common dimensions for",
        default=[],
    ),
    session: Session = Depends(get_session),
) -> List[str]:
    """
    Return common dimensions for a set of metrics.
    """
    metric_nodes = []
    errors = []
    for node_name in metric:
        statement = select(Node).where(Node.name == node_name)
        try:
            node = session.exec(statement).one()
            if node.type != NodeType.METRIC:
                errors.append(
                    DJError(
                        message=f"Not a metric node: {node_name}",
                        code=ErrorCode.NODE_TYPE_ERROR,
                    ),
                )
            metric_nodes.append(node)
        except NoResultFound:
            errors.append(
                DJError(
                    message=f"Metric node not found: {node_name}",
                    code=ErrorCode.UNKNOWN_NODE,
                ),
            )

    if errors:
        raise DJException(errors=errors)
    return list(get_shared_dimensions(metric_nodes))
