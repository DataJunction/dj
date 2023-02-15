"""
Helpers for API endpoints
"""
from http import HTTPStatus
from typing import Dict, List, Optional, Tuple, Union

from fastapi import HTTPException
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from dj.construction.build import build_node_for_database
from dj.construction.dj_query import build_dj_metric_query
from dj.construction.extract import extract_dependencies_from_node
from dj.construction.inference import get_type_of_expression
from dj.errors import DJError, DJException, ErrorCode
from dj.models import Catalog, Column, Database, Engine
from dj.models.node import (
    Node,
    NodeMode,
    NodeRelationship,
    NodeRevision,
    NodeRevisionBase,
    NodeStatus,
    NodeType,
)
from dj.sql.parsing import ast


def get_node_by_name(
    session: Session,
    name: str,
    node_type: Optional[NodeType] = None,
    with_current: bool = False,
) -> Node:
    """
    Get a node by name
    """
    statement = select(Node).where(Node.name == name)
    if node_type:
        statement = statement.where(Node.type == node_type)
    if with_current:
        statement = statement.options(joinedload(Node.current))
        node = session.exec(statement).unique().one_or_none()
    else:
        node = session.exec(statement).one_or_none()

    if not node:
        raise DJException(
            message=(
                f"A {'' if not node_type else node_type + ' '}"
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


async def get_query(
    session: Session,
    metric: str,
    dimensions: List[str],
    filters: List[str],
    database_name: Optional[str] = None,
) -> Tuple[ast.Query, Database]:
    """
    Get a query for a metric, dimensions, and filters
    """
    metric = get_node_by_name(session=session, name=metric, node_type=NodeType.METRIC)
    database_id = (
        get_database_by_name(session=session, name=database_name).id
        if database_name
        else None
    )
    query_ast, optimal_database = await build_node_for_database(
        session=session,
        node=metric.current,
        database_id=database_id,
        dimensions=dimensions,
        filters=filters,
    )
    return query_ast, optimal_database

async def get_dj_query(
    session: Session,
    query: str,
    database_name: Optional[str] = None,
) -> Tuple[ast.Query, Database]:
    """
    Get a query for a metric, dimensions, and filters
    """
    database_id = (
        get_database_by_name(session=session, name=database_name).id
        if database_name
        else None
    )
    query_ast, optimal_database = await build_dj_metric_query(
        session=session,
        query = query,
        database_id=database_id,
    )
    return query_ast, optimal_database

def get_engine(session: Session, name: str, version: str) -> Engine:
    """
    Return an Engine instance given an engine name and version
    """
    statement = (
        select(Engine).where(Engine.name == name).where(Engine.version == version)
    )
    try:
        engine = session.exec(statement).one()
    except NoResultFound as exc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Engine not found: `{name}` version `{version}`",
        ) from exc
    return engine


def get_downstream_nodes(
    session: Session,
    node_name: str,
    node_type: NodeType = None,
) -> List[Node]:
    """
    Gets all downstream children of the given node, filterable by node type.
    Uses a recursive CTE query to build out all descendants from the node.
    """
    node = get_node_by_name(session=session, name=node_name)

    dag = (
        select(
            NodeRelationship.parent_id,
            NodeRevision.node_id,
        )
        .where(NodeRelationship.parent_id == node.id)
        .join(NodeRevision, NodeRelationship.child_id == NodeRevision.id)
        .join(Node, Node.id == NodeRevision.node_id)
    ).cte("dag", recursive=True)

    paths = dag.union_all(
        select(
            dag.c.parent_id,
            NodeRevision.node_id,
        )
        .join(NodeRelationship, dag.c.node_id == NodeRelationship.parent_id)
        .join(NodeRevision, NodeRelationship.child_id == NodeRevision.id)
        .join(Node, Node.id == NodeRevision.node_id),
    )

    statement = (
        select(Node)
        .join(paths, paths.c.node_id == Node.id)
        .options(joinedload(Node.current))
    )

    results = session.exec(statement).unique().all()

    return [
        downstream
        for downstream in results
        if downstream.type == node_type or node_type is None
    ]


def validate_node_data(
    data: Union[NodeRevisionBase, NodeRevision],
    session: Session,
) -> Tuple[Node, NodeRevision, Dict[NodeRevision, List[ast.Table]]]:
    """
    Validate a node.
    """

    node = Node(name=data.name, type=data.type)
    node_revision = NodeRevision.parse_obj(data)
    node_revision.node = node

    # Try to parse the node's query and extract dependencies
    try:
        (
            query_ast,
            dependencies_map,
            missing_parents_map,
        ) = extract_dependencies_from_node(
            session=session,
            node=node_revision,
            raise_=False,
        )
    except ValueError as exc:
        raise DJException(message=str(exc)) from exc

    # Only raise on missing parents if the node mode is set to published
    if missing_parents_map and node_revision.mode == NodeMode.PUBLISHED:
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
    node_revision.columns = [
        Column(name=col.name.name, type=get_type_of_expression(col))  # type: ignore
        for col in query_ast.select.projection
    ]

    node_revision.status = NodeStatus.VALID
    return node, node_revision, dependencies_map
