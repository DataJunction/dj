"""
Helpers for API endpoints
"""
from http import HTTPStatus
from typing import Dict, List, Optional, Tuple, Union

from fastapi import HTTPException
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from dj.construction.build import build_node
from dj.construction.dj_query import build_dj_metric_query
from dj.errors import DJError, DJException, DJInvalidInputException, ErrorCode
from dj.models import AttributeType, Catalog, Column, Engine
from dj.models.attribute import RESERVED_ATTRIBUTE_NAMESPACE
from dj.models.node import (
    MissingParent,
    Node,
    NodeMissingParents,
    NodeMode,
    NodeNamespace,
    NodeRelationship,
    NodeRevision,
    NodeRevisionBase,
    NodeStatus,
    NodeType,
)
from dj.sql.parsing import ast
from dj.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from dj.sql.parsing.backends.exceptions import DJParseException


def get_node_namespace(  # pylint: disable=too-many-arguments
    session: Session,
    namespace: str,
    raise_if_not_exists: bool = True,
) -> str:
    """
    Get a node namespace
    """
    statement = select(NodeNamespace).where(NodeNamespace.namespace == namespace)
    node_namespace = session.exec(statement).one_or_none()
    if raise_if_not_exists:  # pragma: no cover
        if not node_namespace:
            raise DJException(
                message=(f"node namespace `{namespace}` does not exist."),
                http_status_code=404,
            )
    return node_namespace


def get_node_by_name(  # pylint: disable=too-many-arguments
    session: Session,
    name: Optional[str],
    node_type: Optional[NodeType] = None,
    with_current: bool = False,
    raise_if_not_exists: bool = True,
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
    if raise_if_not_exists:
        if not node:
            raise DJException(
                message=(
                    f"A {'' if not node_type else node_type + ' '}"
                    f"node with name `{name}` does not exist."
                ),
                http_status_code=404,
            )
    return node


def raise_if_node_exists(session: Session, name: str) -> None:
    """
    Raise an error if the node with the given name already exists.
    """
    node = get_node_by_name(session, name, raise_if_not_exists=False)
    if node:
        raise DJException(
            message=f"A node with name `{name}` already exists.",
            http_status_code=HTTPStatus.CONFLICT,
        )


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


def get_attribute_type(
    session: Session,
    name: str,
    namespace: Optional[str] = RESERVED_ATTRIBUTE_NAMESPACE,
) -> Optional[AttributeType]:
    """
    Gets an attribute type by name.
    """
    statement = (
        select(AttributeType)
        .where(AttributeType.name == name)
        .where(AttributeType.namespace == namespace)
    )
    return session.exec(statement).one_or_none()


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


def get_query(  # pylint: disable=too-many-arguments
    session: Session,
    node_name: str,
    dimensions: List[str],
    filters: List[str],
) -> ast.Query:
    """
    Get a query for a metric, dimensions, and filters
    """
    node = get_node_by_name(session=session, name=node_name)

    if node.type in (NodeType.DIMENSION, NodeType.SOURCE):
        if dimensions:
            raise DJInvalidInputException(
                message=f"Cannot set dimensions for node type {node.type}!",
            )

    query_ast = build_node(
        session=session,
        node=node.current,
        dialect=None,
        filters=filters,
        dimensions=dimensions,
        build_criteria=None,
    )
    return query_ast


def get_dj_query(
    session: Session,
    query: str,
) -> ast.Query:
    """
    Get a query for a metric, dimensions, and filters
    """

    query_ast = build_dj_metric_query(
        session=session,
        query=query,
    )
    return query_ast


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
) -> Tuple[
    NodeRevision,
    Dict[NodeRevision, List[ast.Table]],
    Dict[str, List[ast.Table]],
    List[str],
]:
    """
    Validate a node.
    """

    if isinstance(data, NodeRevision):
        validated_node = data
    else:
        node = Node(name=data.name, type=data.type)
        validated_node = NodeRevision.parse_obj(data)
        validated_node.node = node
    validated_node.status = NodeStatus.VALID

    # Try to parse the node's query and extract dependencies
    try:
        query_ast = parse(validated_node.query)  # type: ignore
        exc = DJException()
        ctx = ast.CompileContext(session=session, exception=exc)
        dependencies_map, missing_parents_map = query_ast.extract_dependencies(ctx)
    except (ValueError, SqlSyntaxError) as exc:
        raise DJException(message=str(exc)) from exc

    # Only raise on missing parents if the node mode is set to published
    if missing_parents_map:
        if validated_node.mode == NodeMode.DRAFT:
            validated_node.status = NodeStatus.INVALID
        else:
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

    validated_node.columns = []
    type_inference_failed_columns = []
    for col in query_ast.select.projection:
        try:
            column_type = col.type  # type: ignore
            validated_node.columns.append(
                Column(name=col.alias_or_name.name, type=column_type),  # type: ignore
            )
        except DJParseException:
            type_inference_failed_columns.append(col.alias_or_name.name)  # type: ignore
            validated_node.status = NodeStatus.INVALID
    return (
        validated_node,
        dependencies_map,
        missing_parents_map,
        type_inference_failed_columns,
    )


def resolve_downstream_references(
    session: Session,
    node_revision: NodeRevision,
) -> List[NodeRevision]:
    """
    Find all node revisions with missing parent references to `node` and resolve them
    """
    missing_parents = session.exec(
        select(MissingParent).where(MissingParent.name == node_revision.name),
    ).all()
    newly_valid_nodes = []
    for missing_parent in missing_parents:
        missing_parent_links = session.exec(
            select(NodeMissingParents).where(
                NodeMissingParents.missing_parent_id == missing_parent.id,
            ),
        ).all()
        for (
            link
        ) in missing_parent_links:  # Remove from missing parents and add to parents
            downstream_node_id = link.referencing_node_id
            downstream_node_revision = (
                session.exec(
                    select(NodeRevision).where(NodeRevision.id == downstream_node_id),
                )
                .unique()
                .one()
            )
            downstream_node_revision.parents.append(node_revision.node)
            downstream_node_revision.missing_parents.remove(missing_parent)
            (
                _,
                _,
                missing_parents_map,
                type_inference_failed_columns,
            ) = validate_node_data(data=downstream_node_revision, session=session)
            if not missing_parents_map and not type_inference_failed_columns:
                newly_valid_nodes.append(downstream_node_revision)
            session.add(downstream_node_revision)
            session.commit()

        session.delete(missing_parent)  # Remove missing parent reference to node
    return newly_valid_nodes


def propagate_valid_status(
    session: Session,
    valid_nodes: List[NodeRevision],
    catalog_id: int,
) -> None:
    """
    Propagate a valid status by revalidating all downstream nodes
    """
    while valid_nodes:
        resolved_nodes = []
        for node_revision in valid_nodes:
            if node_revision.status != NodeStatus.VALID:
                raise DJException(
                    f"Cannot propagate valid status: Node `{node_revision.name}` is not valid",
                )
            downstream_nodes = get_downstream_nodes(
                session=session,
                node_name=node_revision.name,
            )
            newly_valid_nodes = []
            for node in downstream_nodes:
                (
                    validated_node,
                    _,
                    missing_parents_map,
                    type_inference_failed_columns,
                ) = validate_node_data(data=node.current, session=session)
                if not missing_parents_map and not type_inference_failed_columns:
                    node.current.columns = validated_node.columns or []
                    node.current.status = NodeStatus.VALID
                    node.current.catalog_id = catalog_id
                    session.add(node.current)
                    session.commit()
                    newly_valid_nodes.append(node.current)
            resolved_nodes.extend(newly_valid_nodes)
        valid_nodes = resolved_nodes
