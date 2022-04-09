"""
Node related APIs.
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlmodel import Session, SQLModel, select

from datajunction.models.database import Column
from datajunction.models.node import Node
from datajunction.sql.parse import is_metric
from datajunction.typing import ColumnType
from datajunction.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


class ColumnMetadata(SQLModel):
    """
    Metadata about a column.
    """

    id: Optional[int] = None
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

    expression: Optional[str] = None

    is_metric: bool = False
    columns: List[Column]


@router.get("/nodes/", response_model=List[NodeMetadata])
def read_nodes(*, session: Session = Depends(get_session)) -> List[NodeMetadata]:
    """
    List the available nodes.
    """
    nodes = []
    for node in session.exec(select(Node)):
        enriched_node = NodeMetadata(
            **node.dict(),
            columns=[ColumnMetadata(**column.dict()) for column in node.columns]
        )
        if node.expression:
            enriched_node.is_metric = is_metric(node.expression)

        nodes.append(enriched_node)

    return nodes
