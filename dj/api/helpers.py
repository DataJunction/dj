"""
Helpers for API endpoints
"""

from typing import Optional

from sqlmodel import Session, select

from dj.errors import DJException
from dj.models import Catalog, Column, Database
from dj.models.node import Node, NodeRevision, NodeType


def get_node_by_name(
    session: Session,
    name: str,
    node_type: Optional[NodeType] = None,
) -> Node:
    """
    Get a node by name
    """
    statement = select(Node).where(Node.name == name)
    if node_type:
        statement.where(Node.type == node_type)
    node = session.exec(statement).one_or_none()
    if not node:
        raise DJException(
            message=(
                f"A {'' if not node_type else node_type} "
                f"node with name `{name}` does not exist."
            ),
            http_status_code=404,
        )
    return node


def get_database_by_name(session: Session, name: str) -> Database:
    """
    Get a database by name
    """
    statement = select(Database).where(Database.name == name)
    database = session.exec(statement).one_or_none()
    if not database:
        raise DJException(
            message=f"Database with name `{name}` does not exist.",
            http_status_code=404,
        )
    return database


def get_column(node: NodeRevision, column_name: str) -> Column:
    """
    Get a column from a node revision
    """
    requested_column = None
    for node_column in node.columns:
        if node_column.name == column_name:
            requested_column = node_column
            break

    if not requested_column:
        raise DJException(
            message=f"Column {column_name} does not exist on node {node.name}",
            http_status_code=404,
        )
    return requested_column


def get_catalog(session: Session, name: str) -> Catalog:
    """
    Get a catalog by name
    """
    statement = select(Catalog).where(Catalog.name == name)
    catalog = session.exec(statement).one_or_none()
    if not catalog:
        raise DJException(
            message=f"Catalog with name `{name}` does not exist.",
            http_status_code=404,
        )
    return catalog
