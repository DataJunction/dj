"""
Node related APIs.
"""

import logging
from datetime import datetime
from http import HTTPStatus
from typing import List, Optional, Union

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, SQLModel, select

from dj.models.column import Column, ColumnType
from dj.models.node import Node, NodeBase, NodeEnvironment, NodeType
from dj.models.source import ExistingSourceNode, SourceNodeCreator
from dj.sql.inference import infer_columns
from dj.sql.parse import get_dependencies
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


class NodeCreator(NodeBase):
    """
    A create object for adding a new node
    """


class ExistingNode(NodeBase):
    """
    A node read from the DJ system
    """

    id: int


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

    created_at: datetime
    updated_at: datetime

    type: NodeType
    query: Optional[str] = None

    columns: List[SimpleColumn]


@router.get("/nodes/", response_model=List[NodeMetadata])
def read_nodes(*, session: Session = Depends(get_session)) -> List[NodeMetadata]:
    """
    List the available nodes.
    """
    return session.exec(select(Node)).all()


@router.post("/nodes/", response_model=Union[ExistingSourceNode, ExistingNode])
def create_node(
    data: Union[SourceNodeCreator, NodeCreator],
    session: Session = Depends(get_session),
) -> Union[ExistingSourceNode, ExistingNode]:
    """
    Create a node
    """
    # Check if the node already exists
    query = select(Node).where(Node.name == data.name)
    node = session.exec(query).one_or_none()
    if node:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Node already exists: {data.name}",
        )

    if data.type == NodeType.SOURCE:
        return create_source_node(
            session=session,
            data=data,
        )  # Source nodes can't be created with Node.from_orm
    new_node = Node.from_orm(data)
    new_nodes_parents = get_dependencies(new_node.query) if new_node.query else set()

    # Lookup the parent nodes for the new node
    new_node.parents = session.exec(
        select(Node).where(Node.name.in_(new_nodes_parents)),  # type: ignore  # pylint: disable=no-member
    ).all()
    _logger.info("Found parent nodes for %s: %s", new_node.name, new_node.parents)

    # Infer the node's column names and column types by inspecting the parent nodes
    try:
        new_node.columns = infer_columns(new_node.query, parents=new_node.parents)
    except Exception as e:  # pylint: disable=broad-except
        error_message = f"Cannot infer columns for {new_node.name}: {str(e)}"
        _logger.error(error_message)
        if new_node.environment == NodeEnvironment.PRODUCTION:
            raise HTTPException(
                status_code=HTTPStatus.PRECONDITION_FAILED,
                detail=error_message,
            ) from e

    new_node.extra_validation()

    session.add(new_node)
    session.commit()
    session.refresh(new_node)

    return new_node  # type: ignore


def create_source_node(session: Session, data: SourceNodeCreator) -> Node:
    """
    Create a source node
    """

    config = {
        "name": data.name,
        "description": data.description,
        "type": data.type,
        "environment": data.environment,
        "columns": [
            Column(
                name=column_name,
                type=ColumnType[column_data["type"]],
            )
            for column_name, column_data in data.columns.items()
        ],
        "parents": [],
    }

    new_source_node = Node(**config)
    session.add(new_source_node)
    session.commit()
    session.refresh(new_source_node)

    return new_source_node
