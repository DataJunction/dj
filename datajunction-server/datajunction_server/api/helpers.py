"""
Helpers for API endpoints
"""
import asyncio
import collections
import http.client
import json
import logging
import time
import uuid
from http import HTTPStatus
from typing import Dict, List, Optional, Set, Tuple, Union

from fastapi import HTTPException
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, select

from datajunction_server.construction.build import (
    build_materialized_cube_node,
    build_metric_nodes,
    build_node,
)
from datajunction_server.construction.dj_query import build_dj_metric_query
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    ErrorCode,
)
from datajunction_server.models import AttributeType, Catalog, Column, Engine
from datajunction_server.models.attribute import RESERVED_ATTRIBUTE_NAMESPACE
from datajunction_server.models.engine import Dialect
from datajunction_server.models.history import (
    EntityType,
    History,
    status_change_history,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import (
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
from datajunction_server.models.query import ColumnMetadata, QueryWithResults
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.typing import END_JOB_STATES

_logger = logging.getLogger(__name__)


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
    include_inactive: bool = False,
) -> Node:
    """
    Get a node by name
    """
    statement = select(Node).where(Node.name == name)
    if not include_inactive:
        statement = statement.where(is_(Node.deactivated_at, None))
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


def raise_if_node_inactive(session: Session, name: str) -> None:
    """
    Raise an error if the node with the given name exists and is inactive.
    """
    node = get_node_by_name(
        session,
        name,
        raise_if_not_exists=False,
        include_inactive=True,
    )
    if node and node.deactivated_at:
        raise DJException(
            message=f"Node `{name}` exists but has been deactivated.",
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
    orderby: List[str],
    limit: Optional[int] = None,
    engine: Optional[Engine] = None,
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
        orderby=orderby,
        limit=limit,
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
        select(Engine)
        .where(Engine.name == name)
        .where(Engine.version == (version or ""))
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
    node = get_node_by_name(session=session, name=node_name, include_inactive=True)

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


def find_bound_dimensions(
    validated_node: NodeRevision,
    dependencies_map: Dict[NodeRevision, List[ast.Table]],
) -> Tuple[Set, List[Column]]:
    """
    Finds the matched bound dimensions
    """
    invalid_required_dimensions = set()
    matched_bound_columns = []
    for col in validated_node.required_dimensions:
        names = col.split(".")
        parent_name, column_name = ".".join(names[:-1]), names[-1]

        found_parent_col = False
        for parent in dependencies_map.keys():
            if found_parent_col:
                break  # pragma: no cover
            if (parent.name) != parent_name:
                continue  # pragma: no cover
            for parent_col in parent.columns:
                if parent_col.name == column_name:
                    found_parent_col = True
                    matched_bound_columns.append(parent_col)
                    break
        if not found_parent_col:
            invalid_required_dimensions.add(col)
    return invalid_required_dimensions, matched_bound_columns


def validate_node_data(  # pylint: disable=too-many-locals
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
    if missing_parents_map and validated_node.mode != NodeMode.DRAFT:
        _logger.error(
            "Node %s missing parents %s",
            validated_node.name,
            list(missing_parents_map.keys()),
        )
        raise DJException(
            http_status_code=HTTPStatus.BAD_REQUEST,
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

    column_mapping = {col.name: col for col in validated_node.columns}
    validated_node.columns = []
    type_inference_failures = {}
    for col in query_ast.select.projection:
        column_name = col.alias_or_name.name  # type: ignore
        column = column_mapping.get(column_name)
        try:
            column_type = str(col.type)  # type: ignore
            if column is None:
                column = Column(name=column_name, type=column_type)
            column.type = column_type  # type: ignore
        except DJParseException as parse_exc:
            type_inference_failures[column_name] = parse_exc.message
        if column:
            validated_node.columns.append(column)

    # check that bound dimensions are from parent nodes
    invalid_required_dimensions, matched_bound_columns = find_bound_dimensions(
        validated_node,
        dependencies_map,
    )
    validated_node.required_dimensions = matched_bound_columns

    # Only raise on missing parents or type inference if the node mode is set to published
    if missing_parents_map or type_inference_failures or invalid_required_dimensions:
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
            invalid_required_dimensions_error = (
                [
                    DJError(
                        code=ErrorCode.INVALID_COLUMN,
                        message=(
                            "Node definition contains references to columns as "
                            "required dimensions that are not on parent nodes."
                        ),
                        debug={
                            "invalid_required_dimensions": list(
                                invalid_required_dimensions,
                            ),
                        },
                    ),
                ]
                if invalid_required_dimensions
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
                http_status_code=HTTPStatus.BAD_REQUEST,
                errors=missing_parents_error
                + type_inference_error
                + invalid_required_dimensions_error,
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
                    session.add(
                        status_change_history(
                            node.current,
                            NodeStatus.INVALID,
                            NodeStatus.VALID,
                        ),
                    )
                    session.add(node.current)
                    session.commit()
                    newly_valid_nodes.append(node.current)
            resolved_nodes.extend(newly_valid_nodes)
        valid_nodes = resolved_nodes


def validate_cube(
    session: Session,
    metric_names: List[str],
    dimension_names: List[str],
) -> Tuple[List[Column], List[Node], List[Node], List[Column], Optional[Catalog]]:
    """
    Validate that a set of metrics and dimensions can be built together.
    """
    metrics: List[Column] = []
    metric_nodes: List[Node] = []
    dimension_nodes: List[Node] = []
    dimensions: List[Column] = []
    catalogs = []
    catalog = None

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
        catalog = metric_node.current.catalog
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

    return metrics, metric_nodes, dimension_nodes, dimensions, catalog


def get_history(
    session: Session,
    entity_type: EntityType,
    entity_name: str,
    offset: int,
    limit: int,
):
    """
    Get the history for a given entity type and name
    """
    return session.exec(
        select(History)
        .where(History.entity_type == entity_type)
        .where(History.entity_name == entity_name)
        .offset(offset)
        .limit(limit),
    ).all()


def validate_orderby(
    orderby: List[str],
    metrics: List[str],
    dimension_attributes: List[str],
):
    """
    Validate that all elements in an order by match a metric or dimension attribute
    """
    invalid_orderbys = []
    for orderby_element in orderby:
        if orderby_element not in metrics + dimension_attributes:
            invalid_orderbys.append(orderby_element)
    if invalid_orderbys:
        raise DJException(
            message=(
                f"Columns {invalid_orderbys} in order by clause must also be "
                "specified in the metrics or dimensions"
            ),
        )


def find_existing_cube(
    session: Session,
    metric_columns: List[Column],
    dimension_columns: List[Column],
    materialized: bool = True,
) -> Optional[NodeRevision]:
    """
    Find an existing cube with these metrics and dimensions, if any.
    If `materialized` is set, it will only look for materialized cubes.
    """
    element_names = [col.name for col in (metric_columns + dimension_columns)]
    statement = select(NodeRevision)
    for name in element_names:
        statement = statement.filter(
            NodeRevision.cube_elements.any(Column.name == name),  # type: ignore  # pylint: disable=no-member
        )

    existing_cubes = session.exec(statement).unique().all()
    for cube in existing_cubes:
        if not materialized or (  # pragma: no cover
            materialized and cube.materializations and cube.availability
        ):
            return cube
    return None


def build_sql_for_multiple_metrics(  # pylint: disable=too-many-arguments,too-many-locals
    session: Session,
    metrics: List[str],
    dimensions: List[str],
    filters: List[str] = None,
    orderby: List[str] = None,
    limit: Optional[int] = None,
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> Tuple[TranslatedSQL, Engine, Catalog]:
    """
    Build SQL for multiple metrics. Used by both /sql and /data endpoints
    """
    if not filters:
        filters = []
    if not orderby:
        orderby = []

    metric_columns, metric_nodes, _, dimension_columns, _ = validate_cube(
        session,
        metrics,
        dimensions,
    )
    leading_metric_node = get_node_by_name(session, metrics[0])
    available_engines = leading_metric_node.current.catalog.engines

    # Try to find a built cube that already has the given metrics and dimensions
    # The cube needs to have a materialization configured and an availability state
    # posted in order for us to use the materialized datasource
    cube = find_existing_cube(
        session,
        metric_columns,
        dimension_columns,
        materialized=True,
    )
    if cube:
        catalog = get_catalog(session, cube.availability.catalog)  # type: ignore
        available_engines = catalog.engines + available_engines

    # Check if selected engine is available, or if none is provided, select the fastest
    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {metrics[0]}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )

    validate_orderby(orderby, metrics, dimensions)

    if cube and cube.materializations and cube.availability:
        query_ast = build_materialized_cube_node(
            metric_columns,
            dimension_columns,
            cube,
        )
        return (
            TranslatedSQL(
                sql=str(query_ast),
                columns=[
                    ColumnMetadata(name=col.name, type=str(col.type))
                    for col in (metric_columns + dimension_columns)
                ],
                dialect=engine.dialect,
            ),
            engine,
            cube.catalog,
        )

    query_ast = build_metric_nodes(
        session,
        metric_nodes,
        filters=filters or [],
        dimensions=dimensions or [],
        orderby=orderby or [],
        limit=limit,
    )
    columns = [
        ColumnMetadata(name=col.alias_or_name.name, type=str(col.type))  # type: ignore
        for col in query_ast.select.projection
    ]
    return (
        TranslatedSQL(
            sql=str(query_ast),
            columns=columns,
            dialect=engine.dialect if engine else None,
        ),
        engine,
        leading_metric_node.current.catalog,
    )


async def query_event_stream(  # pylint: disable=too-many-arguments
    query: QueryWithResults,
    query_service_client: QueryServiceClient,
    columns: List[Column],
    request,
    timeout: float = 0.0,
    stream_delay: float = 0.5,
    retry_timeout: int = 5000,
):
    """
    A generator of events from a query submitted to the query service
    """
    starting_time = time.time()
    # Start with query and query_next as the initial state of the query
    query_prev = query_next = query
    query_id = query_prev.id
    _logger.info("sending initial event to the client for query %s", query_id)
    yield {
        "event": "message",
        "id": uuid.uuid4(),
        "retry": retry_timeout,
        "data": json.dumps(query.json()),
    }
    # Continuously check the query until it's complete
    while not timeout or (time.time() - starting_time < timeout):
        # Check if the client closed the connection
        if await request.is_disconnected():  # pragma: no cover
            _logger.error("connection closed by the client")
            break

        # Check the current state of the query
        query_next = query_service_client.get_query(query_id=query_id)  # type: ignore
        if query_next.state in END_JOB_STATES:
            _logger.info(
                "query end state detected (%s), sending final event to the client",
                query_next.state,
            )
            if query_next.results.__root__:  # pragma: no cover
                query_next.results.__root__[0].columns = columns or []
            yield {
                "event": "message",
                "id": uuid.uuid4(),
                "retry": retry_timeout,
                "data": json.dumps(query_next.json()),
            }
            _logger.info("connection closed by the server")
            break
        if query_prev != query_next:  # pragma: no cover
            _logger.info(
                "query information has changed, sending an event to the client",
            )
            yield {
                "event": "message",
                "id": uuid.uuid4(),
                "retry": retry_timeout,
                "data": json.dumps(query_next.json()),
            }

            query = query_next
        await asyncio.sleep(stream_delay)  # pragma: no cover
