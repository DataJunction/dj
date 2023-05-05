"""
Helpers for API endpoints
"""
import collections
import http.client
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
from dj.models.engine import Dialect
from dj.models.node import (
    BuildCriteria,
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
    engine: Optional[Engine],
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

    # Builds the node for the engine's dialect if one is set or defaults to Spark
    if (
        not engine
        and node.current
        and node.current.catalog
        and node.current.catalog.engines
    ):
        engine = node.current.catalog.engines[0]
    build_criteria = BuildCriteria(
        dialect=(engine.dialect if engine and engine.dialect else Dialect.SPARK),
    )

    query_ast = build_node(
        session=session,
        node=node.current,
        filters=filters,
        dimensions=dimensions,
        build_criteria=build_criteria,
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


def get_upstream_nodes(
    session: Session,
    node_name: str,
    node_type: NodeType = None,
) -> List[Node]:
    """
    Gets all upstream parents of the given node, filterable by node type.
    """
    node = get_node_by_name(session=session, name=node_name)
    queue = collections.deque([node])
    all_parents = set()
    while queue:
        node = queue.pop()
        for parent in node.current.parents:
            all_parents.add(parent)
        for parent in node.current.parents:
            queue.append(parent)

    return [
        upstream
        for upstream in all_parents
        if upstream.type == node_type or node_type is None
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

    ctx = ast.CompileContext(session=session, exception=DJException())

    # Try to parse the node's query and extract dependencies
    try:
        query_ast = parse(validated_node.query)  # type: ignore
        dependencies_map, missing_parents_map = query_ast.extract_dependencies(ctx)
    except (ValueError, SqlSyntaxError) as raised_exceptions:
        raise DJException(message=str(raised_exceptions)) from raised_exceptions

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
    type_inference_failures = {}
    for col in query_ast.select.projection:
        try:
            column_type = col.type  # type: ignore
            validated_node.columns.append(
                Column(name=col.alias_or_name.name, type=column_type),  # type: ignore
            )
        except DJParseException as parse_exc:
            type_inference_failures[col.alias_or_name.name] = parse_exc.message  # type: ignore
            validated_node.status = NodeStatus.INVALID

    # Only raise on missing parents or type inference if the node mode is set to published
    if missing_parents_map or type_inference_failures:
        if validated_node.mode == NodeMode.DRAFT:
            validated_node.status = NodeStatus.INVALID
        else:
            missing_parents_error = (
                [
                    DJError(
                        code=ErrorCode.MISSING_PARENT,
                        message="Node definition contains references to nodes that do not exist",
                        debug={"missing_parents": list(missing_parents_map.keys())},
                    ),
                ]
                if missing_parents_map
                else []
            )
            type_inference_error = (
                [
                    DJError(
                        code=ErrorCode.TYPE_INFERENCE,
                        message=(
                            f"Unable to infer type for some columns on node `{data.name}`"
                        ),
                        debug={
                            "columns": type_inference_failures,
                            "errors": ctx.exception.errors,
                        },
                    ),
                ]
                if type_inference_failures
                else []
            )
            raise DJException(
                errors=missing_parents_error + type_inference_error,
            )

    return (
        validated_node,
        dependencies_map,
        missing_parents_map,
        list(type_inference_failures.keys()),
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


def validate_cube(
    session: Session,
    metric_names: List[str],
    dimension_names: List[str],
) -> Tuple[List[Column], List[Node], List[Node], List[Column]]:
    """
    Validate that a set of metrics and dimensions can be built together.
    """
    metrics: List[Column] = []
    metric_nodes: List[Node] = []
    dimension_nodes: List[Node] = []
    dimensions: List[Column] = []
    catalogs = []

    # Verify that the provided metrics are metric nodes
    for node_name in metric_names:
        metric_node = get_node_by_name(session=session, name=node_name)
        if metric_node.type != NodeType.METRIC:
            raise DJException(
                message=(
                    f"Node {metric_node.name} of type {metric_node.type} "
                    f"cannot be added to a cube."
                    + " Did you mean to add a dimension attribute?"
                    if metric_node.type == NodeType.DIMENSION
                    else ""
                ),
                http_status_code=http.client.UNPROCESSABLE_ENTITY,
            )
        catalogs.append(metric_node.current.catalog.name)
        metrics.append(metric_node.current.columns[0])
        metric_nodes.append(metric_node)

    if not metrics:
        raise DJException(
            message=("At least one metric is required"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )

    # Verify that the provided dimension attributes exist
    for dimension_attribute in dimension_names:
        node_name, column_name = dimension_attribute.rsplit(".", 1)
        dimension_node = get_node_by_name(session=session, name=node_name)
        dimension_nodes.append(dimension_node)
        columns = {col.name: col for col in dimension_node.current.columns}
        if column_name in columns:  # pragma: no cover
            dimensions.append(columns[column_name])

    if not dimensions:
        raise DJException(
            message=("At least one dimension is required"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )

    if len(set(catalogs)) > 1:
        raise DJException(
            message=(
                f"Metrics and dimensions cannot be from multiple catalogs: {catalogs}"
            ),
        )

    if len(set(catalogs)) < 1:  # pragma: no cover
        raise DJException(
            message=("Metrics and dimensions must be part of a common catalog"),
        )

    return metrics, metric_nodes, dimension_nodes, dimensions
