# pylint: disable=too-many-lines
"""
Helpers for API endpoints
"""
import asyncio
import http.client
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from http import HTTPStatus
from typing import Dict, List, Optional, Set, Tuple, Union

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.operators import is_

from datajunction_server.construction.build import (
    build_materialized_cube_node,
    build_metric_nodes,
    build_node,
    rename_columns,
    validate_shared_dimensions,
)
from datajunction_server.construction.dj_query import build_dj_query
from datajunction_server.database.attributetype import AttributeType
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import (
    MissingParent,
    Node,
    NodeMissingParents,
    NodeRevision,
)
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidInputException,
    DJNodeNotFound,
    ErrorCode,
)
from datajunction_server.internal.engines import get_engine
from datajunction_server.models import access
from datajunction_server.models.attribute import RESERVED_ATTRIBUTE_NAMESPACE
from datajunction_server.models.base import labelize
from datajunction_server.models.engine import Dialect
from datajunction_server.models.history import status_change_history
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node import BuildCriteria, NodeRevisionBase, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import ColumnMetadata, QueryWithResults
from datajunction_server.naming import LOOKUP_CHARS
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import get_downstream_nodes, get_nodes_with_dimension
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.typing import END_JOB_STATES, UTCDatetime
from datajunction_server.utils import SEPARATOR

_logger = logging.getLogger(__name__)

COLUMN_NAME_REGEX = r"([A-Za-z0-9_\.]+)(\[[A-Za-z0-9_]+\])?"


def get_node_namespace(  # pylint: disable=too-many-arguments
    session: Session,
    namespace: str,
    raise_if_not_exists: bool = True,
) -> NodeNamespace:
    """
    Get a node namespace
    """
    statement = select(NodeNamespace).where(NodeNamespace.namespace == namespace)
    node_namespace = session.execute(statement).scalar_one_or_none()
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
    for_update: bool = False,
) -> Node:
    """
    Get a node by name
    """
    statement = select(Node).where(Node.name == name)
    if for_update:
        statement = statement.with_for_update().execution_options(
            populate_existing=True,
        )
    if not include_inactive:
        statement = statement.where(is_(Node.deactivated_at, None))
    if node_type:
        statement = statement.where(Node.type == node_type)
    if with_current:
        statement = statement.options(joinedload(Node.current)).options(
            joinedload(Node.tags),
        )
        node = session.execute(statement).unique().scalar_one_or_none()
    else:
        node = session.execute(statement).scalar_one_or_none()
    if raise_if_not_exists:
        if not node:
            raise DJNodeNotFound(
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
    return session.execute(statement).scalar_one_or_none()


def get_catalog_by_name(session: Session, name: str) -> Catalog:
    """
    Get a catalog by name
    """
    statement = select(Catalog).where(Catalog.name == name)
    catalog = session.execute(statement).scalar()
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
    access_control: Optional[access.AccessControlStore] = None,
) -> ast.Query:
    """
    Get a query for a metric, dimensions, and filters
    """
    node = get_node_by_name(session=session, name=node_name)

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
        access_control=access_control,
    )
    query_ast = rename_columns(query_ast, node.current)
    return query_ast


def find_bound_dimensions(
    validated_node: NodeRevision,
    dependencies_map: Dict[NodeRevision, List[ast.Table]],
) -> Tuple[Set[str], List[Column]]:
    """
    Finds the matched required dimensions
    """
    invalid_required_dimensions = set()
    matched_bound_columns = []
    required_dimensions_mapping = {}
    for col in validated_node.required_dimensions:
        column_name = col.name if isinstance(col, Column) else col
        for parent in dependencies_map.keys():
            parent_columns = {
                parent_col.name: parent_col for parent_col in parent.columns
            }
            required_dimensions_mapping[column_name] = parent_columns.get(column_name)
    for column_name, required_column in required_dimensions_mapping.items():
        if required_column is not None:
            matched_bound_columns.append(required_column)
        else:
            invalid_required_dimensions.add(column_name)
    return invalid_required_dimensions, matched_bound_columns  # type: ignore


@dataclass
class NodeValidator:
    """
    Node validation
    """

    status: NodeStatus = NodeStatus.VALID
    columns: List[Column] = field(default_factory=list)
    required_dimensions: List[Column] = field(default_factory=list)
    dependencies_map: Dict[NodeRevision, List[ast.Table]] = field(default_factory=dict)
    missing_parents_map: Dict[str, List[ast.Table]] = field(default_factory=dict)
    type_inference_failures: List[str] = field(default_factory=list)
    errors: List[DJError] = field(default_factory=list)

    def differs_from(self, node_revision: NodeRevision):
        """
        Compared to the provided node revision, returns whether the validation
        indicates that the nodes differ.
        """
        if node_revision.status != self.status:
            return True
        existing_columns_map = {col.name: col for col in self.columns}
        for col in node_revision.columns:
            if col.name not in existing_columns_map:
                return True  # pragma: no cover
            if existing_columns_map[col.name].type != col.type:
                return True  # pragma: no cover
        return False

    def modified_columns(self, node_revision: NodeRevision):
        """
        Compared to the provided node revision, returns the modified columns
        """
        initial_node_columns = {col.name: col for col in node_revision.columns}
        updated_columns = set(initial_node_columns.keys()).difference(
            {n.name for n in self.columns},
        )
        for column in self.columns:
            if column.name in initial_node_columns:
                if initial_node_columns[column.name].type != column.type:
                    updated_columns.add(column.name)  # pragma: no cover
            else:  # pragma: no cover
                updated_columns.add(column.name)  # pragma: no cover
        return updated_columns


def validate_node_data(  # pylint: disable=too-many-locals,too-many-statements
    data: Union[NodeRevisionBase, NodeRevision],
    session: Session,
) -> NodeValidator:
    """
    Validate a node. This function should never raise any errors.
    It will build the lists of issues (including errors) and return them all
    for the caller to decide what to do.
    """
    node_validator = NodeValidator()

    if isinstance(data, NodeRevision):
        validated_node = data
    else:
        node = Node(name=data.name, type=data.type)
        validated_node = NodeRevision(**data.dict())
        validated_node.node = node

    ctx = ast.CompileContext(session=session, exception=DJException())

    # Try to parse the node's query, extract dependencies and missing parents
    # dependencies_map = missing_parents_map = {}
    try:
        formatted_query = (
            NodeRevision.format_metric_alias(
                validated_node.query,  # type: ignore
                validated_node.name,
            )
            if validated_node.type == NodeType.METRIC
            else validated_node.query
        )
        query_ast = parse(formatted_query)  # type: ignore
        (
            dependencies_map,
            missing_parents_map,
        ) = query_ast.bake_ctes().extract_dependencies(ctx)
        node_validator.dependencies_map = dependencies_map
        node_validator.missing_parents_map = missing_parents_map
    except (DJParseException, ValueError, SqlSyntaxError) as raised_exceptions:
        node_validator.status = NodeStatus.INVALID
        node_validator.errors.append(
            DJError(code=ErrorCode.INVALID_SQL_QUERY, message=str(raised_exceptions)),
        )
        return node_validator

    # Add aliases for any unnamed columns and confirm that all column types can be inferred
    query_ast.select.add_aliases_to_unnamed_columns()

    # Invalid parents will invalidate this node
    # Note: we include source nodes here because they sometimes appear to be invalid, but
    # this is a bug that needs to be fixed
    invalid_parents = {
        parent.name
        for parent in node_validator.dependencies_map
        if parent.type != NodeType.SOURCE and parent.status == NodeStatus.INVALID
    }
    if invalid_parents:
        node_validator.status = NodeStatus.INVALID

    column_mapping = {col.name: col for col in validated_node.columns}
    node_validator.columns = []
    type_inference_failures = {}
    for idx, col in enumerate(query_ast.select.projection):
        column = None
        column_name = col.alias_or_name.name  # type: ignore
        existing_column = column_mapping.get(column_name)
        try:
            column_type = str(col.type)  # type: ignore
            column = Column(
                name=column_name,
                display_name=labelize(column_name),
                type=column_type,
                attributes=existing_column.attributes if existing_column else [],
                dimension=existing_column.dimension if existing_column else None,
                order=idx,
            )
        except DJParseException as parse_exc:
            type_inference_failures[column_name] = parse_exc.message
            node_validator.status = NodeStatus.INVALID
        except TypeError:  # pragma: no cover
            type_inference_failures[
                column_name
            ] = f"Unknown TypeError on column {column_name}."
            node_validator.status = NodeStatus.INVALID
        if column:
            node_validator.columns.append(column)

    # check that bound dimensions are from parent nodes
    invalid_required_dimensions, matched_bound_columns = find_bound_dimensions(
        validated_node,
        dependencies_map,
    )
    node_validator.required_dimensions = matched_bound_columns

    if missing_parents_map or type_inference_failures or invalid_required_dimensions:
        # update status
        node_validator.status = NodeStatus.INVALID
        # build errors
        missing_parents_error = (
            [
                DJError(
                    code=ErrorCode.MISSING_PARENT,
                    message=f"Node definition contains references to nodes that do not "
                    f"exist: {','.join(missing_parents_map.keys())}",
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
                        f"Unable to infer type for some columns on node `{data.name}`.\n"
                        + ("\n\t* " if type_inference_failures else "")
                        + "\n\t* ".join(
                            [val[:103] for val in type_inference_failures.values()],
                        )
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
        errors = (
            missing_parents_error
            + type_inference_error
            + invalid_required_dimensions_error
        )
        node_validator.errors.extend(errors)

    return node_validator


def resolve_downstream_references(
    session: Session,
    node_revision: NodeRevision,
    current_user: Optional[User] = None,
) -> List[NodeRevision]:
    """
    Find all node revisions with missing parent references to `node` and resolve them
    """
    missing_parents = (
        session.execute(
            select(MissingParent).where(MissingParent.name == node_revision.name),
        )
        .scalars()
        .all()
    )
    newly_valid_nodes = []
    for missing_parent in missing_parents:
        missing_parent_links = (
            session.execute(
                select(NodeMissingParents).where(
                    NodeMissingParents.missing_parent_id == missing_parent.id,
                ),
            )
            .scalars()
            .all()
        )
        for (
            link
        ) in missing_parent_links:  # Remove from missing parents and add to parents
            downstream_node_id = link.referencing_node_id
            downstream_node_revision = (
                session.execute(
                    select(NodeRevision).where(NodeRevision.id == downstream_node_id),
                )
                .unique()
                .scalar_one()
            )
            downstream_node_revision.parents.append(node_revision.node)
            downstream_node_revision.missing_parents.remove(missing_parent)
            node_validator = validate_node_data(
                data=downstream_node_revision,
                session=session,
            )
            event = None
            if downstream_node_revision.status != node_validator.status:
                event = status_change_history(
                    downstream_node_revision,
                    downstream_node_revision.status,
                    node_validator.status,
                    parent_node=node_revision.name,
                    current_user=current_user,
                )

            downstream_node_revision.status = node_validator.status
            downstream_node_revision.columns = node_validator.columns
            if node_validator.status == NodeStatus.VALID:
                newly_valid_nodes.append(downstream_node_revision)
            session.add(downstream_node_revision)
            if event:
                session.add(event)
            session.commit()
            session.refresh(downstream_node_revision)

        session.delete(missing_parent)  # Remove missing parent reference to node
    return newly_valid_nodes


def propagate_valid_status(
    session: Session,
    valid_nodes: List[NodeRevision],
    catalog_id: int,
    current_user: Optional[User] = None,
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
                node_validator = validate_node_data(
                    data=node.current,
                    session=session,
                )
                node.current.status = node_validator.status
                if node_validator.status == NodeStatus.VALID:
                    node.current.columns = node_validator.columns or []
                    node.current.status = NodeStatus.VALID
                    node.current.catalog_id = catalog_id
                    session.add(
                        status_change_history(
                            node.current,
                            NodeStatus.INVALID,
                            NodeStatus.VALID,
                            current_user=current_user,
                        ),
                    )
                    newly_valid_nodes.append(node.current)
                session.add(node.current)
                session.commit()
                session.refresh(node.current)
            resolved_nodes.extend(newly_valid_nodes)
        valid_nodes = resolved_nodes


def map_dimensions_to_roles(dimensions: List[str]) -> Dict[str, str]:
    """
    Returns a mapping between dimension attributes and their roles.
    For example, ["default.users.user_id[user]"] would turn into
    {"default.users.user_id": "[user]"}
    """
    dimension_roles = [re.findall(COLUMN_NAME_REGEX, dim)[0] for dim in dimensions]
    return {dim_rols[0]: dim_rols[1] for dim_rols in dimension_roles}


def validate_cube(  # pylint: disable=too-many-locals
    session: Session,
    metric_names: List[str],
    dimension_names: List[str],
    require_dimensions: bool = True,
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
        try:
            node_name, column_name = dimension_attribute.rsplit(".", 1)
            dimension_node = get_node_by_name(session=session, name=node_name)
        except (ValueError, DJNodeNotFound) as exc:  # pragma: no cover
            raise DJException(
                f"Please make sure that `{dimension_attribute}` "
                "is a dimensional attribute.",
            ) from exc
        dimension_nodes.append(dimension_node)
        columns = {col.name: col for col in dimension_node.current.columns}

        column_name_without_role = column_name
        match = re.fullmatch(COLUMN_NAME_REGEX, column_name)
        if match:
            column_name_without_role = match.groups()[0]

        if column_name_without_role in columns:  # pragma: no cover
            dimensions.append(columns[column_name_without_role])

    if require_dimensions and not dimensions:
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

    validate_shared_dimensions(
        session,
        metric_nodes,
        dimension_names,
        [],
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
    return (
        session.execute(
            select(History)
            .where(History.entity_type == entity_type)
            .where(History.entity_name == entity_name)
            .offset(offset)
            .limit(limit),
        )
        .scalars()
        .all()
    )


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
        if orderby_element.split(" ")[0] not in metrics + dimension_attributes:
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

    existing_cubes = session.execute(statement).unique().scalars().all()
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
    access_control: Optional[access.AccessControlStore] = None,
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
        require_dimensions=False,
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
        catalog = get_catalog_by_name(session, cube.availability.catalog)  # type: ignore
        available_engines = catalog.engines + available_engines

    # Check if selected engine is available
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
        if access_control:  # pragma: no cover
            access_control.add_request_by_node(cube)
            access_control.state = access.AccessControlState.INDIRECT
            access_control.raise_if_invalid_requests()
        materialized_cube_catalog = get_catalog_by_name(
            session,
            cube.availability.catalog,
        )
        query_ast = build_materialized_cube_node(  # pylint: disable=E1121
            metric_columns,
            dimension_columns,
            cube,
            filters,
            orderby,
            limit,
        )
        query_metric_columns = [
            ColumnMetadata(
                name=col.name,
                type=str(col.type),
                column=col.name,
                node=col.node_revision().name,  # type: ignore
            )
            for col in metric_columns
        ]
        query_dimension_columns = [
            ColumnMetadata(
                name=(col.node_revision().name + SEPARATOR + col.name).replace(  # type: ignore
                    SEPARATOR,
                    f"_{LOOKUP_CHARS.get(SEPARATOR)}_",
                ),
                type=str(col.type),
                node=col.node_revision().name,  # type: ignore
                column=col.name,  # type: ignore
            )
            for col in dimension_columns
        ]
        return (
            TranslatedSQL(
                sql=str(query_ast),
                columns=query_metric_columns + query_dimension_columns,
                dialect=materialized_cube_catalog.engines[0].dialect,
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
        access_control=access_control,
    )
    columns = [
        assemble_column_metadata(col)  # type: ignore
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
        query_next = query_service_client.get_query(  # type: ignore # pragma: no cover
            query_id=query_id,
        )
        if query_next.state in END_JOB_STATES:  # pragma: no cover
            _logger.info(  # pragma: no cover
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


def build_sql_for_dj_query(  # pylint: disable=too-many-arguments,too-many-locals
    session: Session,
    query: str,
    access_control: access.AccessControl,
    engine_name: Optional[str] = None,
    engine_version: Optional[str] = None,
) -> Tuple[TranslatedSQL, Engine, Catalog]:
    """
    Build SQL for multiple metrics. Used by /djsql endpoints
    """

    query_ast, dj_nodes = build_dj_query(session, query)

    for node in dj_nodes:
        access_control.add_request_by_node(
            node.current,
        )

    access_control.validate_and_raise()

    leading_metric_node = dj_nodes[0]
    available_engines = leading_metric_node.current.catalog.engines

    # Check if selected engine is available
    engine = (
        get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )

    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {leading_metric_node.name}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
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


def deactivate_node(
    session: Session,
    name: str,
    message: str = None,
    current_user: Optional[User] = None,
):
    """
    Deactivates a node and propagates to all downstreams.
    """
    node = get_node_by_name(session, name, with_current=True)

    # Find all downstream nodes and mark them as invalid
    downstreams = get_downstream_nodes(session, node.name)
    for downstream in downstreams:
        if downstream.current.status != NodeStatus.INVALID:
            downstream.current.status = NodeStatus.INVALID
            session.add(
                status_change_history(
                    downstream.current,
                    NodeStatus.VALID,
                    NodeStatus.INVALID,
                    parent_node=node.name,
                    current_user=current_user,
                ),
            )
            session.add(downstream)

    now = datetime.utcnow()
    node.deactivated_at = UTCDatetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )
    session.add(node)
    session.add(
        History(
            entity_type=EntityType.NODE,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.DELETE,
            details={"message": message} if message else {},
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()


def activate_node(
    session: Session,
    name: str,
    message: str = None,
    current_user: Optional[User] = None,
):
    """Restores node and revalidate all downstreams."""
    node = get_node_by_name(session, name, with_current=True, include_inactive=True)
    if not node.deactivated_at:
        raise DJException(
            http_status_code=HTTPStatus.BAD_REQUEST,
            message=f"Cannot restore `{name}`, node already active.",
        )
    node.deactivated_at = None  # type: ignore

    # Find all downstream nodes and revalidate them
    downstreams = get_downstream_nodes(session, node.name)
    for downstream in downstreams:
        old_status = downstream.current.status
        if downstream.type == NodeType.CUBE:
            downstream.current.status = NodeStatus.VALID
            for element in downstream.current.cube_elements:
                if (
                    element.node_revisions
                    and element.node_revisions[-1].status == NodeStatus.INVALID
                ):  # pragma: no cover
                    downstream.current.status = NodeStatus.INVALID
        else:
            # We should not fail node restoration just because of some nodes
            # that have been invalid already and stay that way.
            node_validator = validate_node_data(downstream.current, session)
            downstream.current.status = node_validator.status
            if node_validator.errors:
                downstream.current.status = NodeStatus.INVALID
        session.add(downstream)
        if old_status != downstream.current.status:
            session.add(
                status_change_history(
                    downstream.current,
                    old_status,
                    downstream.current.status,
                    parent_node=node.name,
                    current_user=current_user,
                ),
            )

    session.add(node)
    session.add(
        History(
            entity_type=EntityType.NODE,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.RESTORE,
            details={"message": message} if message else {},
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()


def revalidate_node(
    name: str,
    session: Session,
    parent_node: str = None,
    current_user: Optional[User] = None,
):
    """
    Revalidate a single existing node and update its status appropriately
    """
    node = get_node_by_name(session, name)
    current_node_revision = node.current
    if current_node_revision.type == NodeType.SOURCE:
        if current_node_revision.status != NodeStatus.VALID:  # pragma: no cover
            current_node_revision.status = NodeStatus.VALID
            session.add(
                status_change_history(
                    current_node_revision,
                    NodeStatus.INVALID,
                    NodeStatus.VALID,
                    current_user=current_user,
                ),
            )
            session.add(current_node_revision)
            session.commit()
            session.refresh(current_node_revision)
        return NodeStatus.VALID
    previous_status = current_node_revision.status
    node_validator = validate_node_data(current_node_revision, session)
    current_node_revision.status = node_validator.status
    if previous_status != current_node_revision.status:  # pragma: no cover
        session.add(current_node_revision)
        session.add(
            status_change_history(
                current_node_revision,
                previous_status,
                current_node_revision.status,
                parent_node=parent_node,
                current_user=current_user,
            ),
        )
        session.commit()
        session.refresh(current_node_revision)
        session.refresh(node)
    return current_node_revision.status


def hard_delete_node(
    name: str,
    session: Session,
    current_user: Optional[User] = None,
):
    """
    Hard delete a node, destroying all links and invalidating all downstream nodes.
    This should be used with caution, deactivating a node is preferred.
    """
    node = get_node_by_name(session, name, with_current=True, include_inactive=True)
    downstream_nodes = get_downstream_nodes(session=session, node_name=name)

    linked_nodes = []
    if node.type == NodeType.DIMENSION:
        linked_nodes = get_nodes_with_dimension(session=session, dimension_node=node)

    session.delete(node)
    session.commit()
    impact = []  # Aggregate all impact of this deletion to include in response

    # Revalidate all downstream nodes
    for node in downstream_nodes:
        session.add(  # Capture this in the downstream node's history
            History(
                entity_type=EntityType.DEPENDENCY,
                entity_name=name,
                node=node.name,
                activity_type=ActivityType.DELETE,
                user=current_user.username if current_user else None,
            ),
        )
        status = revalidate_node(
            name=node.name,
            session=session,
            parent_node=name,
            current_user=current_user,
        )
        impact.append(
            {
                "name": node.name,
                "status": status,
                "effect": "downstream node is now invalid",
            },
        )

    # Revalidate all linked nodes
    for node in linked_nodes:
        session.add(  # Capture this in the downstream node's history
            History(
                entity_type=EntityType.LINK,
                entity_name=name,
                node=node.name,
                activity_type=ActivityType.DELETE,
                user=current_user.username if current_user else None,
            ),
        )
        status = revalidate_node(
            name=node.name,
            session=session,
            current_user=current_user,
        )
        impact.append(
            {
                "name": node.name,
                "status": status,
                "effect": "broken link",
            },
        )
    session.add(  # Capture this in the downstream node's history
        History(
            entity_type=EntityType.NODE,
            entity_name=name,
            node=name,
            activity_type=ActivityType.DELETE,
            details={
                "impact": impact,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()  # Commit the history events
    return impact


def assemble_column_metadata(
    column: ast.Column,
    # node_name: Union[List[str], str],
) -> ColumnMetadata:
    """
    Extract column metadata from AST
    """
    metadata = ColumnMetadata(
        name=column.alias_or_name.name,
        type=str(column.type),
        column=(
            column.semantic_entity.split(SEPARATOR)[-1]
            if hasattr(column, "semantic_entity") and column.semantic_entity
            else None
        ),
        node=(
            SEPARATOR.join(column.semantic_entity.split(SEPARATOR)[:-1])
            if hasattr(column, "semantic_entity") and column.semantic_entity
            else None
        ),
        semantic_entity=column.semantic_entity
        if hasattr(column, "semantic_entity")
        else None,
        semantic_type=column.semantic_type
        if hasattr(column, "semantic_type")
        else None,
    )
    return metadata
