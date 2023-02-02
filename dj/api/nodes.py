"""
Node related APIs.
"""

import logging
from typing import List, Optional, Union

from fastapi import APIRouter, Depends
from sqlmodel import Session, SQLModel, select

from dj.construction.extract import extract_dependencies_from_node
from dj.construction.inference import get_type_of_expression
from dj.errors import DJError, DJException, ErrorCode
from dj.models.column import Column, ColumnType
from dj.models.node import (
    AvailabilityState,
    Node,
    NodeBase,
    NodeMode,
    NodeStatus,
    NodeType,
)
from dj.utils import UTCDatetime, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


class SimpleColumn(SQLModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    type: ColumnType


class NodeMetadata(SQLModel):
    """
    A node with information about columns and if it is a metric.
    """

    id: int
    name: str
    description: str = ""

    created_at: UTCDatetime
    updated_at: UTCDatetime

    type: NodeType
    query: Optional[str] = None
    availability: Optional[AvailabilityState] = None
    columns: List[SimpleColumn]


class NodeValidation(SQLModel):
    """
    A validation of a provided node definition
    """

    message: str
    status: NodeStatus
    node: Node
    dependencies: List[NodeMetadata]
    columns: List[Column]


@router.post("/nodes/validate/", response_model=NodeValidation)
def validate_node(
    data: Union[NodeBase],
    session: Session = Depends(get_session),
) -> NodeValidation:
    """
    Validate a node.
    """

    if data.type == NodeType.SOURCE:
        raise DJException(message="Source nodes cannot be validated")
    node = Node.from_orm(data)

    # Try to parse the node's query and extract dependencies
    try:
        (
            query_ast,
            dependencies_map,
            missing_parents_map,
        ) = extract_dependencies_from_node(
            session=session,
            node=node,
            raise_=False,
        )
    except ValueError as exc:
        raise DJException(message=str(exc)) from exc

    # Only raise on missing parents if the node mode is set to published
    if missing_parents_map and node.mode == NodeMode.PUBLISHED:
        raise DJException(
            errors=[
                DJError(
                    code=ErrorCode.MISSING_PARENT,
                    message="Node definition contains references to nodes that do not exist",
                    debug={"missing_parents": list(missing_parents_map.keys())},
                ),
            ],
        )

    # Add aliases for any unnamed columns and confirm that all column types can be inferred
    query_ast.select.add_aliases_to_unnamed_columns()
    node.columns = [
        Column(name=col.name.name, type=get_type_of_expression(col))  # type: ignore
        for col in query_ast.select.projection
    ]

    node.status = NodeStatus.VALID
    return NodeValidation(
        message=f"Node `{node.name}` is valid",
        status=NodeStatus.VALID,
        node=node,
        dependencies=set(dependencies_map.keys()),
        columns=node.columns,
    )


@router.get("/nodes/", response_model=List[NodeMetadata])
def read_nodes(*, session: Session = Depends(get_session)) -> List[NodeMetadata]:
    """
    List the available nodes.
    """
    return session.exec(select(Node)).all()
