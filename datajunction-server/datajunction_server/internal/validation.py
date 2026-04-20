"""Node validation functions."""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Union

from sqlalchemy.exc import MissingGreenlet
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import find_required_dimensions
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.column import Column, ColumnAttribute
from datajunction_server.errors import (
    DJError,
    DJException,
    DJInvalidMetricQueryException,
    ErrorCode,
)
from datajunction_server.internal.deployment.type_inference import validate_node_query
from datajunction_server.internal.deployment.utils import (
    classify_parents,
    extract_upstream_candidates,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.node import NodeRevisionBase, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.instrumentation.provider import timed
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import SqlSyntaxError, parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.types import ListType, MapType, StructType


def _reparse_parent_column_types(dependencies_map: Dict) -> None:
    """Re-parse string column types on parent nodes before type inference.

    Columns loaded from the DB with exotic types (map<...>, array<...>) may
    arrive as plain strings if they bypassed the normal parse path. This
    ensures those types are fully parsed before the AST type-inference walk.
    If a type string is unparseable, the original value is left unchanged.
    """
    from datajunction_server.sql.parsing.backends.antlr4 import parse_rule

    for parent in dependencies_map.keys():
        for col in parent.columns:
            if isinstance(col.type, str) or (
                type(col.type).__name__ == "ColumnType"
                and not isinstance(col.type, (MapType, ListType, StructType))
            ):
                try:
                    col.type = parse_rule(str(col.type), "dataType")
                except Exception:
                    # If parsing fails, leave the original type
                    pass


def update_ast_column_types(node: ast.Node) -> None:
    """Recursively update AST Column._type from parsed database column types.

    Call this after re-parsing parent column types (via parse_rule) so that
    type inference on the query AST sees the properly-typed objects rather than
    raw ColumnType strings.
    """
    if isinstance(node, ast.Column) and node._table and hasattr(node._table, "dj_node"):
        dj_node = node._table.dj_node  # type: ignore[attr-defined]
        for db_col in dj_node.columns:
            if db_col.name == node.name.name:
                node._type = db_col.type
                break
    for child in node.children:
        update_ast_column_types(child)


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


@timed(
    "dj.node_validation.ms",
    lambda data, session: {"node_type": str(data.type)},
)
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

        # Collect table/subquery aliases and CTE names before bake_ctes() destroys them.
        # Both Table nodes (regular tables) and Query nodes (inline subqueries) can
        # carry aliases that appear as column namespaces in the query body.
        local_aliases: Set[str] = set()
        for tbl in (*query_ast.find_all(ast.Table), *query_ast.find_all(ast.Query)):
            if tbl.alias is not None:
                local_aliases.add(tbl.alias.identifier(False))
        for cte in query_ast.ctes:
            local_aliases.add(cte.alias_or_name.identifier(False))

        # Lambda parameters (e.g. `c` in `c -> c.name = ...`) are also valid namespaces
        # inside their lambda body and must be excluded from INVALID_COLUMN checks.
        for lambda_expr in query_ast.find_all(ast.Lambda):
            for ident in lambda_expr.identifiers:
                local_aliases.add(ident.name)
        (
            dependencies_map,
            missing_parents_map,
        ) = await query_ast.bake_ctes().extract_dependencies(ctx)
        node_validator.dependencies_map = dependencies_map
        node_validator.missing_parents_map = missing_parents_map

        # compile() runs inside extract_dependencies. Surface any INVALID_COLUMN
        # errors it produced, filtering out references whose namespace is a local
        # SQL alias or CTE — those are valid and resolved at full compile time.
        # Dimension attribute references where the node exists but the column isn't
        # formally registered are already handled in compile() itself (clean return,
        # no error appended), so no extra DB lookup is needed here.
        invalid_col_errors = [
            e for e in ctx.exception.errors if e.code == ErrorCode.INVALID_COLUMN
        ]
        unresolved = [
            e
            for e in invalid_col_errors
            if (ns := (e.debug or {}).get("namespace", "")) and ns not in local_aliases
        ]
        if unresolved:
            node_validator.status = NodeStatus.INVALID
            node_validator.errors.extend(unresolved)

    except (DJParseException, ValueError, SqlSyntaxError) as raised_exceptions:
        node_validator.status = NodeStatus.INVALID
        node_validator.errors.append(
            DJError(code=ErrorCode.INVALID_SQL_QUERY, message=str(raised_exceptions)),
        )
        return node_validator

    # Parse parent column types before type inference
    _reparse_parent_column_types(dependencies_map)

    # Update AST Column nodes with the newly parsed types.
    update_ast_column_types(query_ast)

    # Add aliases for any unnamed columns and confirm that all column types can be inferred
    query_ast.select.add_aliases_to_unnamed_columns()

    if validated_node.type == NodeType.METRIC and node_validator.dependencies_map:
        # Check if this is a derived metric (references other metrics)
        metric_parents = [
            parent
            for parent in node_validator.dependencies_map.keys()
            if parent.type == NodeType.METRIC
        ]
        non_metric_parents = [
            parent
            for parent in node_validator.dependencies_map.keys()
            if parent.type != NodeType.METRIC
        ]

        if metric_parents:
            # This is a derived metric - nested derived metrics are supported
            # via inline expansion during decomposition
            if len(metric_parents) > 1:
                # For cross-fact derived metrics, validate that there are shared dimensions
                # between all referenced base metrics
                from datajunction_server.sql.dag import get_dimensions

                # Get dimensions for each base metric
                all_dimension_sets: List[Set[str]] = []
                for base_metric in metric_parents:
                    dims = await get_dimensions(
                        session,
                        base_metric.node,
                        with_attributes=True,
                    )
                    dim_names = {d.name for d in dims}
                    all_dimension_sets.append(dim_names)

                # Compute intersection of all dimension sets
                if all_dimension_sets:  # pragma: no branch
                    shared_dimensions = all_dimension_sets[0]
                    for dim_set in all_dimension_sets[1:]:
                        shared_dimensions = shared_dimensions & dim_set

                    if not shared_dimensions:
                        metric_names = [m.name for m in metric_parents]
                        node_validator.status = NodeStatus.INVALID
                        node_validator.errors.append(
                            DJError(
                                code=ErrorCode.INVALID_PARENT,
                                message=(
                                    f"Cannot create derived metric from base metrics with no shared "
                                    f"dimensions. The following metrics have no dimensions in common: "
                                    f"{', '.join(metric_names)}. Cross-fact derived metrics require "
                                    f"at least one shared dimension for joining."
                                ),
                            ),
                        )
        else:
            # Standard metric - validate columns exist on parent nodes
            all_available_columns = {
                col.name
                for upstream_node in non_metric_parents
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
            # Use the parsed ColumnType object directly instead of converting to string
            # This ensures proper type information (MapType, ListType, etc.) is preserved
            column_type = col.type  # type: ignore
            column = Column(
                name=column_name.lower()
                if validated_node.type != NodeType.METRIC
                else column_name,
                display_name=existing_column.display_name
                if existing_column and existing_column.display_name
                else labelize(column_name),
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

    # Find required dimension columns from full dimension paths
    # e.g., "dimensions.date.dateint" -> find column "dateint" on node "dimensions.date"
    try:
        # Get parent columns for short name lookups (already parsed above)
        parent_columns = [
            col for parent in dependencies_map.keys() for col in parent.columns
        ]
        # Get required dimensions as strings (may be Column objects if already resolved)
        required_dim_strings = [
            col.full_name() if isinstance(col, Column) else col
            for col in validated_node.required_dimensions
        ]
        (
            invalid_required_dimensions,
            matched_bound_columns,
        ) = await find_required_dimensions(
            session,
            required_dim_strings,
            parent_columns,
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


# ---------------------------------------------------------------------------
# New validate_node_data — shares primitives with the deployment path.
# ---------------------------------------------------------------------------


def _format_query_for_validation(validated_node: NodeRevision) -> str:
    """Apply metric-aliasing when needed so the parsed AST matches the shape
    validate_node_query expects (mirrors the legacy path)."""
    if validated_node.type == NodeType.METRIC:
        return NodeRevision.format_metric_alias(
            validated_node.query,  # type: ignore
            validated_node.name,
        )
    return validated_node.query  # type: ignore


# _map_validation_error intentionally removed — see internal/deployment/validation.py
# bulk_validate_node_data which wraps every validate_node_query error with
# ErrorCode.TYPE_INFERENCE and does no further filtering. The single-node path
# does the same below so its error surface matches deployment exactly.


def _build_columns_from_output(
    output_columns: list,
    query_ast: ast.Query,
    validated_node: NodeRevision,
) -> tuple[list[Column], dict[str, str]]:
    """Build Column objects for every AST projection item.

    validate_node_query drops columns whose type resolution raised (e.g., unresolved
    dim-attribute refs) — legacy compile() produced all of them via DB lookups.
    To preserve the legacy column list we walk the AST projection directly for
    ordering and names, then look up types from output_columns where available.
    Missing types fall back to UnknownType so downstream consumers still see the
    column entry.
    """
    from datajunction_server.sql.parsing.types import UnknownType

    try:
        column_mapping = {col.name: col for col in validated_node.columns}
    except MissingGreenlet:  # pragma: no cover
        column_mapping = {}  # pragma: no cover

    types_by_name = {name: col_type for name, col_type in output_columns}

    columns: list[Column] = []
    type_inference_failures: dict[str, str] = {}
    for idx, expr in enumerate(query_ast.select.projection):  # type: ignore[attr-defined]
        col_name = expr.alias_or_name.name  # type: ignore[union-attr]
        col_type = types_by_name.get(col_name, UnknownType())
        existing = column_mapping.get(col_name)
        columns.append(
            Column(
                name=col_name.lower()
                if validated_node.type != NodeType.METRIC
                else col_name,
                display_name=existing.display_name
                if existing and existing.display_name
                else labelize(col_name),
                type=col_type,
                attributes=[
                    ColumnAttribute(
                        attribute_type_id=attr.attribute_type_id,
                        attribute_type=attr.attribute_type,
                    )
                    for attr in existing.attributes
                ]
                if existing
                else [],
                dimension=existing.dimension if existing else None,
                order=idx,
            ),
        )
    return columns, type_inference_failures


@timed(
    "dj.node_validation.v2.ms",
    lambda data, session: {"node_type": str(data.type)},
)
async def validate_node_data_v2(
    data: Union[NodeRevisionBase, NodeRevision],
    session: AsyncSession,
) -> NodeValidator:
    """
    New node validator — shares primitives (extract_upstream_candidates,
    classify_parents, validate_node_query) with the deployment path so behavior
    matches bulk deployment.

    Not wired to any caller yet. Exposed alongside legacy validate_node_data
    for shadow-mode diff scripts to compare results. Once divergences are
    triaged and fixed, swap names and delete the legacy impl.
    """
    node_validator = NodeValidator()

    # Wrap NodeRevisionBase into a NodeRevision for internal consistency
    if isinstance(data, NodeRevision):
        validated_node = data
    else:
        node = Node(name=data.name, type=data.type)
        validated_node = NodeRevision(**data.model_dump())
        validated_node.node = node

    # --- Step 1: parse (metric-aliased if needed) ---
    try:
        formatted_query = _format_query_for_validation(validated_node)
        query_ast = parse(formatted_query)  # type: ignore
    except (DJParseException, ValueError, SqlSyntaxError) as exc:
        node_validator.status = NodeStatus.INVALID
        node_validator.errors.append(
            DJError(code=ErrorCode.INVALID_SQL_QUERY, message=str(exc)),
        )
        return node_validator

    # Stable col{n} names for unnamed projections (matches legacy behavior)
    query_ast.select.add_aliases_to_unnamed_columns()

    # --- Step 2: extract upstream candidates (SHARED with deployment) ---
    is_metric = validated_node.type == NodeType.METRIC
    candidates = extract_upstream_candidates(query_ast, is_metric=is_metric)

    # --- Step 3: bulk load parents (ONE DB query instead of per-table compile) ---
    # Use the default load options so dependencies can be serialized by API
    # response models that access .availability, .materializations, .parents,
    # .dimension_links (legacy compile implicitly traversed these).
    dep_nodes: Dict[str, Node] = {}
    if candidates:
        loaded = await Node.get_by_names(session, sorted(candidates))
        dep_nodes = {n.name: n for n in loaded}

    # --- Step 4: classify parents (SHARED with deployment) ---
    is_derived_metric = is_metric and query_ast.select.from_ is None
    parents, missing = classify_parents(
        is_derived_metric,
        candidates,
        dep_nodes,
    )

    node_validator.dependencies_map = {
        parent.current: [] for parent in parents if parent.current
    }
    node_validator.missing_parents_map = {name: [] for name in missing}

    # --- Step 5: re-parse exotic column types before type inference ---
    _reparse_parent_column_types(node_validator.dependencies_map)

    # --- Step 6: SQL validation + type inference ---
    parent_columns_map: dict[str, dict] = {}
    for parent in parents:
        if parent.current and parent.current.columns:
            parent_columns_map[parent.name] = {
                col.name: col.type for col in parent.current.columns
            }
    # Include missing parents with empty columns so validate_node_query doesn't
    # choke on unknown-table errors for refs we've already tracked.
    for name in missing:
        parent_columns_map.setdefault(name, {})

    validation = validate_node_query(
        formatted_query,
        parent_columns_map,
        pre_parsed=query_ast,
    )

    # --- Step 7: surface every validate_node_query error directly. Same code
    # and no filtering as bulk_validate_node_data does for deployment — this is
    # the parity point. If tightening or codes need to diverge later, revisit.
    if validation.errors:
        for msg in validation.errors:
            node_validator.errors.append(
                DJError(code=ErrorCode.TYPE_INFERENCE, message=msg),
            )
        node_validator.status = NodeStatus.INVALID

    # --- Step 8: build columns from output_columns ---
    columns, type_inference_failures = _build_columns_from_output(
        validation.output_columns,
        query_ast,
        validated_node,
    )
    node_validator.columns = columns
    if type_inference_failures:
        node_validator.status = NodeStatus.INVALID

    # --- Step 9: metric-specific checks (cross-fact shared-dim + MISSING_COLUMNS) ---
    if is_metric and node_validator.dependencies_map:
        metric_parents = [
            parent
            for parent in node_validator.dependencies_map.keys()
            if parent.type == NodeType.METRIC
        ]
        non_metric_parents = [
            parent
            for parent in node_validator.dependencies_map.keys()
            if parent.type != NodeType.METRIC
        ]

        if metric_parents and len(metric_parents) > 1:
            # Cross-fact derived metric: all base metrics must share >=1 dimension
            from datajunction_server.sql.dag import get_dimensions

            all_dimension_sets: List[Set[str]] = []
            for base_metric in metric_parents:
                dims = await get_dimensions(
                    session,
                    base_metric.node,
                    with_attributes=True,
                )
                all_dimension_sets.append({d.name for d in dims})

            if all_dimension_sets:  # pragma: no branch
                shared = all_dimension_sets[0]
                for ds in all_dimension_sets[1:]:
                    shared = shared & ds
                if not shared:
                    names = [m.name for m in metric_parents]
                    node_validator.status = NodeStatus.INVALID
                    node_validator.errors.append(
                        DJError(
                            code=ErrorCode.INVALID_PARENT,
                            message=(
                                f"Cannot create derived metric from base metrics with no shared "
                                f"dimensions. The following metrics have no dimensions in common: "
                                f"{', '.join(names)}. Cross-fact derived metrics require "
                                f"at least one shared dimension for joining."
                            ),
                        ),
                    )
        elif not metric_parents:
            # Standard metric: SELECT cols must exist on non-metric parents
            all_available_columns = {
                col.name
                for upstream_node in non_metric_parents
                for col in upstream_node.columns
            }
            metric_expression = query_ast.select.projection[0]
            referenced_columns = metric_expression.find_all(ast.Column)
            missing_columns = [
                col.alias_or_name.name
                for col in referenced_columns
                if not col.namespace
                and col.alias_or_name.name not in all_available_columns
            ]
            if missing_columns:
                node_validator.status = NodeStatus.INVALID
                node_validator.errors.append(
                    DJError(
                        code=ErrorCode.MISSING_COLUMNS,
                        message=(
                            f"Metric definition references missing columns: "
                            f"{', '.join(missing_columns)}"
                        ),
                    ),
                )

    # --- Step 10: invalid-parent check ---
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

    # --- Step 11: required dimensions ---
    try:
        parent_columns = [
            col
            for parent in node_validator.dependencies_map.keys()
            for col in parent.columns
        ]
        required_dim_strings = [
            col.full_name() if isinstance(col, Column) else col
            for col in validated_node.required_dimensions
        ]
        (
            invalid_required_dimensions,
            matched_bound_columns,
        ) = await find_required_dimensions(
            session,
            required_dim_strings,
            parent_columns,
        )
        node_validator.required_dimensions = matched_bound_columns
    except MissingGreenlet:  # pragma: no cover
        invalid_required_dimensions = set()
        node_validator.required_dimensions = []

    # --- Step 12: final error assembly for missing parents, type-inference, and
    #              invalid required dims (matches legacy code shapes) ---
    if (
        node_validator.missing_parents_map
        or type_inference_failures
        or invalid_required_dimensions
    ):
        node_validator.status = NodeStatus.INVALID
        if node_validator.missing_parents_map:
            node_validator.errors.append(
                DJError(
                    code=ErrorCode.MISSING_PARENT,
                    message=(
                        f"Node definition contains references to nodes that do not "
                        f"exist: {','.join(node_validator.missing_parents_map.keys())}"
                    ),
                    debug={
                        "missing_parents": list(
                            node_validator.missing_parents_map.keys(),
                        ),
                    },
                ),
            )
        for column, message in type_inference_failures.items():
            node_validator.errors.append(
                DJError(
                    code=ErrorCode.TYPE_INFERENCE,
                    message=message,
                    debug={"columns": [column]},
                ),
            )
        if invalid_required_dimensions:
            node_validator.errors.append(
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
            )

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
    if not projection_0.is_aggregation() and query_ast.select.from_ is not None:  # type: ignore
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
