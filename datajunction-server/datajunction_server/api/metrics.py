"""
Metric related APIs.
"""

from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, HTTPException, Query
from sqlalchemy.exc import NoResultFound
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, select

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.api.nodes import list_nodes
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import User, access
from datajunction_server.models.metric import Metric
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    MetricDirection,
    MetricMetadataOptions,
    MetricUnit,
    Node,
    NodeType,
)
from datajunction_server.sql.dag import get_shared_dimensions
from datajunction_server.utils import get_current_user, get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["metrics"])


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


@router.get("/metrics/", response_model=List[str])
def list_metrics(
    prefix: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> List[str]:
    """
    List all available metrics.
    """
    return list_nodes(
        node_type=NodeType.METRIC,
        prefix=prefix,
        session=session,
        current_user=current_user,
        validate_access=validate_access,
    )


@router.get("/metrics/metadata")
def list_metric_metadata() -> MetricMetadataOptions:
    """
    Return available metric metadata attributes
    """
    return_obj = MetricMetadataOptions(
        directions=[MetricDirection(e) for e in MetricDirection],
        units=[MetricUnit(e).value for e in MetricUnit],
    )
    return return_obj


@router.get("/metrics/{name}/", response_model=Metric)
def get_a_metric(name: str, *, session: Session = Depends(get_session)) -> Metric:
    """
    Return a metric by name.
    """
    node = get_metric(session, name)
    metric = Metric.parse_node(node)
    return metric


@router.get(
    "/metrics/common/dimensions/",
    response_model=List[DimensionAttributeOutput],
)
async def get_common_dimensions(
    metric: List[str] = Query(
        title="List of metrics to find common dimensions for",
        default=[],
    ),
    session: Session = Depends(get_session),
) -> List[DimensionAttributeOutput]:
    """
    Return common dimensions for a set of metrics.
    """
    metric_nodes = []
    errors = []
    for node_name in metric:
        statement = (
            select(Node)
            .where(Node.name == node_name)
            .where(is_(Node.deactivated_at, None))
        )
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
    return get_shared_dimensions(metric_nodes)
