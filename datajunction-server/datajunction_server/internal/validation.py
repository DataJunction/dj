"""Node validation functions."""
from dataclasses import dataclass, field
from typing import Dict, List, Set, Union

from sqlalchemy.exc import MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import find_bound_dimensions
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.column import Column
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.models.base import labelize
from datajunction_server.models.node import NodeRevisionBase, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


@dataclass
class NodeValidator:  # pylint: disable=too-many-instance-attributes
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


async def validate_node_data(  # pylint: disable=too-many-locals,too-many-statements
    data: Union[NodeRevisionBase, NodeRevision],
    session: AsyncSession,
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
