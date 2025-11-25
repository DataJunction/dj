"""Node validation functions."""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Union

from sqlalchemy.exc import MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import find_bound_dimensions
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.column import Column, ColumnAttribute
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidMetricQueryException,
    ErrorCode,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.node import NodeRevisionBase, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


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
    updated_columns: List[str] = field(default_factory=list)

    def modified_columns(self, node_revision: NodeRevision) -> Set[str]:
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


async def validate_node_data(
    data: Union[NodeRevisionBase, NodeRevision],
    session: AsyncSession,
) -> NodeValidator:
    """
    Validate a node. This function should never raise any errors.
    It will build the lists of issues (including errors) and return them all
    for the caller to decide what to do.
    """
    node_validator = NodeValidator()

    # Create context without bulk loading for new nodes
    ctx = ast.CompileContext(session=session, exception=DJException())

    if isinstance(data, NodeRevision):
        validated_node = data
        # await session.refresh(data, ["parents"])
        # ctx = await create_compile_context_with_bulk_deps(
        #     session=session,
        #     node_names={parent.name for parent in data.parents},
        # )
    else:
        node = Node(name=data.name, type=data.type)
        validated_node = NodeRevision(**data.model_dump())
        validated_node.node = node

    # Try to parse the node's query, extract dependencies and missing parents
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
        ) = await query_ast.bake_ctes().extract_dependencies(ctx)
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

    if validated_node.type == NodeType.METRIC and node_validator.dependencies_map:
        all_available_columns = {
            col.name
            for upstream_node in node_validator.dependencies_map.keys()
            for col in upstream_node.columns
        }

        metric_expression = query_ast.select.projection[0]
        referenced_columns = metric_expression.find_all(ast.Column)

        missing_columns = []
        for col in referenced_columns:
            column_name = col.alias_or_name.name
            # Skip columns with namespaces, those are from dimension links and will
            # be validated when the metric node is compiled
            if not col.namespace and column_name not in all_available_columns:
                missing_columns.append(column_name)

        if missing_columns:
            node_validator.status = NodeStatus.INVALID
            node_validator.errors.append(
                DJError(
                    code=ErrorCode.MISSING_COLUMNS,
                    message=f"Metric definition references missing columns: {', '.join(missing_columns)}",
                ),
            )

    # Invalid parents will invalidate this node
    # Note: we include source nodes here because they sometimes appear to be invalid, but
    # this is a bug that needs to be fixed
    invalid_parents = {
        parent.name
        for parent in node_validator.dependencies_map
        if parent.type != NodeType.SOURCE and parent.status == NodeStatus.INVALID
    }
    if invalid_parents:
        node_validator.errors.append(
            DJError(
                code=ErrorCode.INVALID_PARENT,
                message=f"References invalid parent node(s) {','.join(invalid_parents)}",
            ),
        )
        node_validator.status = NodeStatus.INVALID

    try:
        column_mapping = {col.name: col for col in validated_node.columns}
    except MissingGreenlet:  # pragma: no cover
        column_mapping = {}  # pragma: no cover
    node_validator.columns = []
    type_inference_failures = {}
    for idx, col in enumerate(query_ast.select.projection):  # type: ignore
        column = None
        column_name = col.alias_or_name.name  # type: ignore
        existing_column = column_mapping.get(column_name)
        try:
            column_type = str(col.type)  # type: ignore
            column = Column(
                name=column_name.lower()
                if validated_node.type != NodeType.METRIC
                else column_name,
                display_name=labelize(column_name),
                type=column_type,
                attributes=[
                    ColumnAttribute(
                        attribute_type_id=attr.attribute_type_id,
                        attribute_type=attr.attribute_type,
                    )
                    for attr in existing_column.attributes
                ]
                if existing_column
                else [],
                dimension=existing_column.dimension if existing_column else None,
                order=idx,
            )
        except DJParseException as parse_exc:
            type_inference_failures[column_name] = parse_exc.message
            node_validator.status = NodeStatus.INVALID
        except TypeError:  # pragma: no cover
            type_inference_failures[column_name] = (
                f"Unknown TypeError on column {column_name}."
            )
            node_validator.status = NodeStatus.INVALID
        if column:
            node_validator.columns.append(column)

    # check that bound dimensions are from parent nodes
    try:
        invalid_required_dimensions, matched_bound_columns = find_bound_dimensions(
            validated_node,
            dependencies_map,
        )
        node_validator.required_dimensions = matched_bound_columns
    except MissingGreenlet:
        invalid_required_dimensions = set()
        node_validator.required_dimensions = []

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
                    message=message,
                    debug={
                        "columns": [column],
                        "errors": ctx.exception.errors,
                    },
                )
                for column, message in type_inference_failures.items()
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


def validate_metric_query(query_ast: ast.Query, name: str) -> None:
    """
    Validate a metric query.
    The Node SQL query should have a single expression in its
    projections and it should be an aggregation function.
    """
    if len(query_ast.select.projection) != 1:
        raise DJInvalidMetricQueryException(
            message="Metric queries can only have a single "
            f"expression, found {len(query_ast.select.projection)}",
        )

    projection_0 = query_ast.select.projection[0]
    if not projection_0.is_aggregation():  # type: ignore
        raise DJInvalidMetricQueryException(
            f"Metric {name} has an invalid query, should have an aggregate expression",
        )

    if query_ast.select.where:
        raise DJInvalidMetricQueryException(
            "Metric cannot have a WHERE clause. Please use IF(<clause>, ...) instead",
        )

    clauses = [
        "GROUP BY" if query_ast.select.group_by else None,
        "HAVING" if query_ast.select.having else None,
        "LATERAL VIEW" if query_ast.select.lateral_views else None,
        "UNION or INTERSECT" if query_ast.select.set_op else None,
        "LIMIT" if query_ast.select.limit else None,
        "ORDER BY"
        if query_ast.select.organization and query_ast.select.organization.order
        else None,
        "SORT BY"
        if query_ast.select.organization and query_ast.select.organization.sort
        else None,
    ]
    invalid_clauses = [clause for clause in clauses if clause is not None]
    if invalid_clauses:
        raise DJInvalidMetricQueryException(
            "Metric has an invalid query. The following are not allowed: "
            + ", ".join(invalid_clauses),
        )
