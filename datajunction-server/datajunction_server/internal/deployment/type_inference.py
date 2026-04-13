"""
Lightweight top-down type inference for deployment propagation.

This module resolves output column names and types for a SQL query using
pre-loaded parent column data. No DB calls - all resolution is in-memory.

Used by propagate_impact to cheaply revalidate downstream nodes when an
upstream node's columns change, without going through the full Query.compile
pipeline.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from datajunction_server.utils import SEPARATOR

from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing import ast
from datajunction_server.errors import DJNotImplementedException
from datajunction_server.sql.parsing.types import (
    BooleanType,
    ColumnType,
    IntegerType,
    ListType,
    MapType,
    NullType,
    StringType,
    StructType,
    UnknownType,
)

logger = logging.getLogger(__name__)

_SENTINEL = object()

# Type alias: maps node name → {column_name: ColumnType}
ParentColumnsMap = dict[str, dict[str, ColumnType]]

# Output: list of (column_name, column_type) for the query's projection
OutputColumns = list[tuple[str, ColumnType]]

# Table scope: maps table alias/name → {column_name: ColumnType}
TableScope = dict[str, dict[str, ColumnType]]


@dataclass
class TypeScope:
    """All type information available during resolution."""

    # Tables from FROM clause, CTEs, subqueries, lateral views
    # Keyed by alias or full node name.
    tables: TableScope = field(default_factory=dict)

    # The full parent columns map - includes dimension nodes that may not be
    # in FROM but can be referenced via dimension attributes.
    parent_map: ParentColumnsMap = field(default_factory=dict)

    # Collected errors during resolution (instead of raising immediately)
    errors: list[str] = field(default_factory=list)


@dataclass
class QueryValidationResult:
    """Result of validating a node's query."""

    output_columns: OutputColumns
    errors: list[str] = field(default_factory=list)


class TypeResolutionError(Exception):
    """Raised when type resolution fails (missing table, missing column, etc.)."""


def validate_node_query(
    query_str: str,
    parent_columns_map: ParentColumnsMap,
) -> QueryValidationResult:
    """
    Validate that all column references in a SQL query resolve against
    the provided parent columns, and return the output column types.

    Checks all clauses: SELECT, WHERE, GROUP BY, HAVING, ORDER BY,
    JOIN conditions, and CTE internals.

    Collects all errors rather than stopping at the first one.

    Args:
        query_str: The SQL query to analyze.
        parent_columns_map: Pre-loaded map of parent node names to their
            column name→type mappings.

    Returns:
        QueryValidationResult with output_columns and any errors found.
    """
    try:
        query = parse(query_str)
    except Exception as exc:
        return QueryValidationResult(
            output_columns=[],
            errors=[f"Failed to parse query: {exc}"],
        )

    cte_registry: dict[str, OutputColumns] = {}
    all_errors: list[str] = []

    for cte in query.ctes:
        cte_name = cte.alias_or_name.name
        cte_columns, cte_errors = _resolve_query(cte, parent_columns_map, cte_registry)
        cte_registry[cte_name] = cte_columns
        all_errors.extend(cte_errors)

    output_columns, query_errors = _resolve_query(
        query,
        parent_columns_map,
        cte_registry,
    )
    all_errors.extend(query_errors)

    return QueryValidationResult(
        output_columns=output_columns,
        errors=all_errors,
    )


def _resolve_query(
    query: ast.Query,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
) -> tuple[OutputColumns, list[str]]:
    """Resolve output columns for a single query (may be a subquery or CTE).

    Returns (output_columns, errors) — collects errors instead of raising.
    """
    select = query.select
    if isinstance(select, ast.InlineTable):
        return _resolve_inline_table(query), []

    if not isinstance(select, ast.Select):  # pragma: no cover
        return [], []

    try:
        tables, table_errors = _build_table_scope(
            select,
            parent_columns_map,
            cte_registry,
        )
    except TypeResolutionError as exc:
        return [], [str(exc)]

    scope = TypeScope(tables=tables, parent_map=parent_columns_map, errors=table_errors)

    output: OutputColumns = []
    for expr in select.projection:
        try:
            output.extend(_resolve_projection_expr(expr, scope))
        except Exception as exc:
            scope.errors.append(str(exc))

    # Flag any output columns with unresolved types
    for col_name, col_type in output:
        if isinstance(col_type, UnknownType):
            scope.errors.append(
                f"Unable to infer type for column `{col_name}`. "
                f"This may indicate an unsupported function or expression.",
            )

    # Validate column references in non-projection clauses
    _validate_non_projection_clauses(select, scope)

    # Validate set operations (UNION/EXCEPT/INTERSECT right side)
    if select.set_op and select.set_op.right:
        _, set_op_errors = _resolve_query(
            ast.Query(select=select.set_op.right),
            parent_columns_map,
            cte_registry,
        )
        scope.errors.extend(set_op_errors)

    return output, scope.errors


# ---------------------------------------------------------------------------
# Inline tables (VALUES)
# ---------------------------------------------------------------------------


def _resolve_inline_table(query: ast.Query) -> OutputColumns:
    """Resolve columns for a VALUES expression with explicit column aliases.

    Infers types from the first row of values when available.
    """
    select = query.select
    if not isinstance(select, ast.InlineTable):  # pragma: no cover
        return []

    col_names = (
        [col.alias_or_name.name for col in select._columns] if select._columns else []
    )

    first_row = select.values[0] if select.values else []
    result: OutputColumns = []
    for i, name in enumerate(col_names):
        if i < len(first_row):
            val = first_row[i]
            if isinstance(val, ast.Number):
                result.append((name, val.type))
            elif isinstance(val, ast.String):
                result.append((name, StringType()))
            elif isinstance(val, ast.Boolean):
                result.append((name, BooleanType()))
            else:
                result.append((name, UnknownType()))
        else:
            result.append((name, UnknownType()))
    return result


# ---------------------------------------------------------------------------
# Table scope building
# ---------------------------------------------------------------------------


def _build_table_scope(
    select: ast.Select,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
) -> tuple[TableScope, list[str]]:
    """
    Build a mapping of table alias/name → {column_name: column_type} for all
    tables available in this query's scope. Returns (scope, errors).
    """
    if select.from_ is None:
        return {"__derived__": _build_derived_scope(parent_columns_map)}, []

    scope: TableScope = {}
    errors: list[str] = []
    for relation in select.from_.relations:
        tables, errs = _collect_tables_from_relation(
            relation,
            parent_columns_map,
            cte_registry,
        )
        scope.update(tables)
        errors.extend(errs)
    for view in select.lateral_views:
        scope.update(_collect_lateral_view_columns(view, scope))
    return scope, errors


def _build_derived_scope(parent_columns_map: ParentColumnsMap) -> dict[str, ColumnType]:
    """
    For derived metrics (no FROM clause), build a flat scope that maps
    metric node names to their output types.
    """
    scope: dict[str, ColumnType] = {}
    for node_name, columns in parent_columns_map.items():
        if len(columns) == 1:
            col_type = next(iter(columns.values()))
            scope[node_name] = col_type
        for col_name, col_type in columns.items():
            scope[f"{node_name}.{col_name}"] = col_type
    return scope


def _collect_tables_from_relation(
    node: ast.Node,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
) -> tuple[TableScope, list[str]]:
    """Collect table scopes from a FROM relation. Returns (tables, errors)."""
    result: TableScope = {}
    errors: list[str] = []

    if isinstance(node, ast.Relation):
        tables, errs = _collect_tables_from_relation(
            node.primary,
            parent_columns_map,
            cte_registry,
        )
        result.update(tables)
        errors.extend(errs)
        for ext in node.extensions:
            if isinstance(ext, ast.Join):  # pragma: no branch
                tables, errs = _collect_tables_from_relation(
                    ext.right,
                    parent_columns_map,
                    cte_registry,
                )
                result.update(tables)
                errors.extend(errs)

    elif isinstance(node, ast.Table):
        table_name = node.identifier(quotes=False)
        alias = node.alias.name if node.alias else table_name

        if table_name in cte_registry:
            result[alias] = {name: typ for name, typ in cte_registry[table_name]}
        elif table_name in parent_columns_map:
            result[alias] = dict(parent_columns_map[table_name])
        else:
            raise TypeResolutionError(
                f"Table `{table_name}` not found in parent columns map. "
                f"Available: {list(parent_columns_map.keys())}",
            )

    elif isinstance(node, ast.Query):
        sub_alias = node.alias.name if node.alias else "__subquery__"
        sub_columns, sub_errors = _resolve_query(node, parent_columns_map, cte_registry)
        result[sub_alias] = {name: typ for name, typ in sub_columns}
        errors.extend(sub_errors)

    elif isinstance(node, ast.InlineTable):
        alias = node.alias.name if node.alias else "__inline__"
        inline_columns = _resolve_inline_table(ast.Query(select=node))  # type: ignore[arg-type]
        result[alias] = {name: typ for name, typ in inline_columns}

    elif isinstance(node, ast.FunctionTableExpression):  # pragma: no branch
        alias = node.alias.name if node.alias else "__func_table__"
        func_cols = {col.name.name: UnknownType() for col in (node.column_list or [])}
        if func_cols:  # pragma: no branch
            result[alias] = func_cols

    return result, errors


def _collect_lateral_view_columns(
    view: ast.LateralView,
    from_scope: TableScope,
) -> TableScope:
    """Collect columns from a LATERAL VIEW (e.g., EXPLODE) expression.

    Resolves element types from the source column's ListType/MapType
    when possible, falls back to UnknownType otherwise.
    """
    func = view.func
    alias = func.alias.name if func.alias else "__lateral__"
    col_list = func.column_list or []
    if not col_list:
        return {}

    element_types = _resolve_lateral_element_types(func, from_scope)
    func_name = func.name.name.upper() if hasattr(func, "name") and func.name else ""
    is_posexplode = "POS" in func_name

    lateral_cols: dict[str, ColumnType] = {}
    for i, col in enumerate(col_list):
        if is_posexplode and i == 0:
            lateral_cols[col.name.name] = IntegerType()
        elif is_posexplode and i == 1 and element_types:
            lateral_cols[col.name.name] = element_types[0]
        elif not is_posexplode and i < len(element_types):
            lateral_cols[col.name.name] = element_types[i]
        else:
            lateral_cols[col.name.name] = UnknownType()

    return {alias: lateral_cols}


def _resolve_lateral_element_types(
    func: ast.FunctionTableExpression,
    from_scope: TableScope,
) -> list[ColumnType]:
    """Resolve element types for an EXPLODE/UNNEST function argument."""
    if not func.args:
        return []  # pragma: no cover

    arg = func.args[0]

    # Resolve the argument's type using the standard expression resolver.
    # Handles all cases uniformly:
    #   EXPLODE(tags)            — Column reference to a ListType/MapType column
    #   EXPLODE(SEQUENCE(1, 90)) — Function returning ListType via the registry
    #   EXPLODE(col['key'])      — Subscript on a map/struct
    scope = TypeScope(tables=from_scope, parent_map={})
    try:
        col_type = _resolve_expr_type(arg, scope)
    except (TypeResolutionError, Exception):
        return []

    if isinstance(col_type, ListType):
        return [col_type.element.type]
    if isinstance(col_type, MapType):
        return [col_type.key.type, col_type.value.type]
    return []


# ---------------------------------------------------------------------------
# Projection resolution
# ---------------------------------------------------------------------------


def _resolve_projection_expr(
    expr: ast.Node,
    scope: TypeScope,
) -> OutputColumns:
    """
    Resolve a single projection expression to its output column(s).

    Handles: Column refs, Wildcard, aliases, functions, literals, expressions.
    """
    if isinstance(expr, ast.Wildcard):
        table_alias = (
            expr.namespace[0].name
            if hasattr(expr, "namespace") and expr.namespace
            else None
        )
        return _resolve_wildcard(table_alias, scope.tables)

    # Unwrap Alias(child=..., alias="name") → resolve the child, use the alias as name
    if isinstance(expr, ast.Alias):
        output_name = expr.alias.name if expr.alias else _get_output_name(expr.child)
        child_results = _resolve_projection_expr(expr.child, scope)
        return (
            [(output_name, child_results[0][1])]
            if child_results
            else [(output_name, UnknownType())]
        )

    output_name = _get_output_name(expr)

    if isinstance(expr, ast.Column):
        col_type = _resolve_column_type(expr, scope)
        return [(output_name, col_type)]

    # Scalar subquery in projection: (SELECT ... ) AS alias
    if isinstance(expr, ast.Query):
        sub_columns, sub_errors = _resolve_query(expr, scope.parent_map, {})
        scope.errors.extend(sub_errors)
        if sub_columns:
            return [(output_name, sub_columns[0][1])]
        return [(output_name, UnknownType())]  # pragma: no cover

    # For expressions (Function, BinaryOp, Cast, literals, etc.)
    col_type = _resolve_expr_type(expr, scope)
    return [(output_name, col_type)]


def _resolve_wildcard(
    table_alias: Optional[str],
    tables: TableScope,
) -> OutputColumns:
    """Expand ``*`` or ``t.*`` into columns from the relevant table(s)."""
    if table_alias:
        if table_alias in tables:
            return list(tables[table_alias].items())
        # Alias not found — fall through to expand all tables.
        # This can happen due to a parser bug where the namespace
        # on Wildcard is incorrect.

    result: OutputColumns = []
    for cols in tables.values():
        result.extend(cols.items())
    return result


# ---------------------------------------------------------------------------
# Non-projection clause validation
# ---------------------------------------------------------------------------


def _resolve_struct_chain(
    col_type: ColumnType,
    field_path: list[str],
    root_name: str,
) -> ColumnType:
    """Walk a chain of struct field accesses.

    Given a StructType and a list of field names like ["inner", "value"],
    resolves struct_col.inner.value by walking into nested structs.
    """
    current = col_type
    traversed: list[str] = [root_name]
    for field_name in field_path:
        if not isinstance(current, StructType):  # pragma: no cover
            raise TypeResolutionError(
                f"`{SEPARATOR.join(traversed)}` is not a struct type, "
                f"cannot access field `{field_name}`.",
            )
        if field_name not in current.fields_mapping:
            raise TypeResolutionError(
                f"Field `{field_name}` not found in struct column "
                f"`{SEPARATOR.join(traversed)}`. "
                f"Available fields: {list(current.fields_mapping.keys())}",
            )
        current = current.fields_mapping[field_name].type
        traversed.append(field_name)
    return current


def _resolve_struct_field(
    struct_col_name: str,
    field_path: list[str],
    tables: TableScope,
) -> Optional[ColumnType]:
    """Try to resolve a struct field access like metadata.name or data.inner.value.

    Searches all tables for a column named struct_col_name. If found and
    the column's type is a StructType, walks the field_path chain.
    Returns the resolved type, or raises TypeResolutionError if a field
    doesn't exist. Returns None if no struct column found.
    """
    for table_cols in tables.values():
        if struct_col_name in table_cols:
            col_type = table_cols[struct_col_name]
            if isinstance(col_type, StructType):  # pragma: no branch
                return _resolve_struct_chain(col_type, field_path, struct_col_name)
    return None


def _validate_non_projection_clauses(select: ast.Select, scope: TypeScope):
    """Validate column references in WHERE, GROUP BY, HAVING, ORDER BY, and JOINs.

    Only validates columns that belong to this query's scope — columns inside
    nested subqueries are validated when those subqueries are resolved.

    Appends errors to scope.errors instead of raising.
    """
    clauses_to_check: list[ast.Node] = []

    if select.where:
        clauses_to_check.append(select.where)
    for expr in select.group_by:
        clauses_to_check.append(expr)
    if select.having:
        clauses_to_check.append(select.having)
    if select.organization:
        for item in select.organization.order:
            clauses_to_check.append(item.expr)
        for item in select.organization.sort:
            clauses_to_check.append(item.expr)

    if select.from_:
        for relation in select.from_.relations:
            _collect_join_conditions(relation, clauses_to_check)

    # Window function OVER clauses (PARTITION BY, ORDER BY)
    for proj_item in select.projection:
        proj_node: ast.Node = proj_item  # type: ignore[assignment]
        for func in proj_node.find_all(ast.Function):
            if hasattr(func, "over") and func.over:
                for partition_expr in func.over.partition_by:
                    clauses_to_check.append(partition_expr)
                for sort_item in func.over.order_by:
                    clauses_to_check.append(sort_item.expr)

    for clause in clauses_to_check:
        _validate_columns_in_clause(clause, scope)


def _validate_columns_in_clause(clause: ast.Node, scope: TypeScope):
    """Validate column references in a clause, skipping nested subqueries.

    Walks the AST but stops recursing when it hits a Query or Select node
    (subquery). Appends errors to scope.errors.
    """
    if isinstance(clause, (ast.Query, ast.Select, ast.Lambda)):
        return  # pragma: no cover
    if isinstance(clause, ast.Column):
        try:
            _resolve_column_type(clause, scope)
        except TypeResolutionError as exc:
            scope.errors.append(str(exc))
        return
    for child in clause.children:
        _validate_columns_in_clause(child, scope)


def _collect_join_conditions(node: ast.Node, conditions: list[ast.Node]):
    """Recursively collect JOIN ON conditions from a relation."""
    if isinstance(node, ast.Relation):
        for ext in node.extensions:
            if isinstance(ext, ast.Join):  # pragma: no branch
                if ext.criteria and ext.criteria.on:
                    conditions.append(ext.criteria.on)
                _collect_join_conditions(ext.right, conditions)


# ---------------------------------------------------------------------------
# DJ node column resolution (dimension attribute references)
# ---------------------------------------------------------------------------


def _resolve_dj_node_column(
    col: ast.Column,
    parent_map: ParentColumnsMap,
) -> Optional[ColumnType]:
    """
    Try to resolve a column reference as a DJ node attribute using progressive
    prefix matching against parent_map.

    For a column like ads.report.dim.date.year with namespace [ads, report, dim, date]
    and name "year", tries progressively longer prefixes:
      ads.report.dim.date → check if node, column = year
      ads.report.dim → check if node, column = date.year (struct?)
      ads.report → check if node, column = dim.date.year
      ads → check if node, column = report.dim.date.year

    Returns the column type if found, None otherwise.
    """
    all_parts = [n.name for n in col.namespace] + [col.name.name]

    for split_at in range(len(all_parts) - 1, 0, -1):
        node_name = ".".join(all_parts[:split_at])
        col_name = all_parts[split_at]

        if node_name in parent_map:
            node_cols = parent_map[node_name]
            if col_name in node_cols:
                return node_cols[col_name]

    return None


# ---------------------------------------------------------------------------
# Column type resolution
# ---------------------------------------------------------------------------


def _resolve_column_type(
    col: ast.Column,
    scope: TypeScope,
) -> ColumnType:
    """Resolve a column reference to its type using the scope."""
    col_name = col.name.name

    # Derived metric pattern (no FROM clause)
    if "__derived__" in scope.tables:
        derived = scope.tables["__derived__"]
        full_id = col.identifier()
        if full_id in derived:
            return derived[full_id]
        if col.namespace:  # pragma: no branch
            result = _resolve_dj_node_column(col, scope.parent_map)
            if result is not None:
                return result  # pragma: no cover
        raise TypeResolutionError(
            f"Column `{col}` not found in derived metric scope.",
        )

    # Table-qualified column: namespace.column_name
    if col.namespace:
        table_alias = col.namespace[0].name
        if table_alias in scope.tables:
            if col_name in scope.tables[table_alias]:
                return scope.tables[table_alias][col_name]

            # Multi-part namespace: table.struct_col[.nested...].field
            # e.g., m.details.is_flag or t.data.inner.value
            if len(col.namespace) > 1:
                struct_col_name = col.namespace[1].name
                if struct_col_name in scope.tables[table_alias]:  # pragma: no branch
                    col_type = scope.tables[table_alias][struct_col_name]
                    # Build the remaining field path: namespace[2:] + col_name
                    field_path = [n.name for n in col.namespace[2:]] + [col_name]
                    if isinstance(col_type, StructType):
                        return _resolve_struct_chain(
                            col_type,
                            field_path,
                            struct_col_name,
                        )
                    if not field_path[:-1]:  # pragma: no cover
                        # Only one level deep but not a struct
                        return UnknownType()
                    raise TypeResolutionError(  # pragma: no cover
                        f"`{table_alias}.{struct_col_name}` is not a struct type, "
                        f"cannot access field `{field_path[0]}`.",
                    )

            raise TypeResolutionError(
                f"Column `{col_name}` not found in table `{table_alias}`. "
                f"Available: {list(scope.tables[table_alias].keys())}",
            )

        # Check if this is a struct field access (e.g., metadata.name or
        # data.inner.value). The first namespace part is the column name,
        # remaining namespace parts + col_name form the field path.
        field_path = [n.name for n in col.namespace[1:]] + [col_name]
        struct_result = _resolve_struct_field(table_alias, field_path, scope.tables)
        if struct_result is not None:
            return struct_result

        # Multi-part namespace not matching any FROM table - likely a DJ
        # dimension attribute reference (e.g., ads.report.dim.date.year).
        result = _resolve_dj_node_column(col, scope.parent_map)
        if result is not None:
            return result

        # Not resolvable - return UnknownType since it may be a dimension
        # ref that gets resolved via dimension links at query time.
        return UnknownType()

    # Unqualified column - search all tables
    found_type = None
    found_in = None
    for alias, cols in scope.tables.items():
        if col_name in cols:
            if found_type is not None:
                raise TypeResolutionError(
                    f"Column `{col_name}` is ambiguous - found in "
                    f"`{found_in}` and `{alias}`.",
                )
            found_type = cols[col_name]
            found_in = alias

    if found_type is not None:
        return found_type

    raise TypeResolutionError(
        f"Column `{col_name}` not found in any table. "
        f"Available tables: {list(scope.tables.keys())}",
    )


# ---------------------------------------------------------------------------
# Higher-order function type inference
# ---------------------------------------------------------------------------


def _resolve_higher_order_function(
    func: ast.Function,
    scope: "TypeScope",
) -> Optional[ColumnType]:
    """Infer the return type of higher-order functions (AGGREGATE, FILTER, TRANSFORM).

    These functions use lambda expressions whose parameters (c, acc, etc.) are
    not in the table scope, so the AST's built-in type inference fails.  We
    resolve the return type from the non-lambda arguments instead.

    Returns None if the function is not a known higher-order function or if
    the return type can't be determined.
    """
    name = (
        func.name.name.upper() if hasattr(func.name, "name") else str(func.name).upper()
    )
    args = func.args

    if name == "AGGREGATE" and len(args) >= 2:
        # AGGREGATE(array, initial_value, merge_fn [, finish_fn])
        # Return type = type of initial_value (the accumulator seed).
        # The merge lambda must return the same type, and an optional
        # finish lambda can transform it — but without resolving lambdas
        # the initial value is our best signal.
        return _resolve_expr_type(args[1], scope)

    if name == "FILTER" and len(args) >= 1:
        # FILTER(array, predicate) → same array type as input
        return _resolve_expr_type(args[0], scope)

    if name == "TRANSFORM" and len(args) >= 2:
        # TRANSFORM(array, transform_fn) → array of transform return type.
        # We can't resolve the lambda return type, so fall back to the
        # array element type if it's known, otherwise UnknownType.
        return _resolve_expr_type(args[0], scope)

    return None


# ---------------------------------------------------------------------------
# Expression type resolution
# ---------------------------------------------------------------------------


def _resolve_expr_type(
    expr: ast.Node,
    scope: TypeScope,
) -> ColumnType:
    """
    Resolve the type of an arbitrary expression.

    Delegates to the AST's own type inference where possible (Function.type,
    Cast, literals). For column references, uses the scope.

    Note: may mutate _type on AST Column nodes as a side effect, since the
    AST's built-in type properties (Function.infer_type, BinaryOp.type, etc.)
    read _type from child nodes. The AST is throwaway — parsed fresh per
    validate_node_query call.
    """
    if isinstance(expr, ast.Column):
        return _resolve_column_type(expr, scope)

    if isinstance(expr, ast.Function):
        # Resolve arg types ourselves and stamp _type on each arg AND all
        # nested nodes so the function's dispatch mechanism can read them
        # via .type without re-traversing the uncompiled AST.
        _stamp_types_recursive(expr, scope)
        try:
            result = expr.type
            if result is not None:  # pragma: no branch
                return result
        except (
            TypeError,
            DJParseException,
            DJNotImplementedException,
            KeyError,
            TypeResolutionError,
        ) as exc:
            logger.info(
                "Function type inference failed for %s: %s. Arg types: %s",
                expr.name,
                exc,
                [
                    (type(a).__name__, getattr(a, "_type", "no _type"))
                    for a in expr.args
                ],
            )
        # Higher-order functions: infer return type from args when the AST
        # can't resolve it (lambda body columns aren't in scope).
        hof_result = _resolve_higher_order_function(expr, scope)
        if hof_result is not None:
            return hof_result
        return UnknownType()

    if isinstance(expr, ast.Cast):
        return expr.data_type

    if isinstance(expr, ast.Number):
        return expr.type

    if isinstance(expr, ast.String):
        return StringType()

    if isinstance(expr, ast.Boolean):
        return BooleanType()

    if isinstance(expr, ast.Null):
        return NullType()

    if isinstance(expr, ast.BinaryOp):
        left_type = _resolve_expr_type(expr.left, scope)
        _resolve_expr_type(expr.right, scope)
        try:
            return expr.type
        except (DJParseException, TypeError, AttributeError):
            return (
                left_type if not isinstance(left_type, UnknownType) else UnknownType()
            )

    if isinstance(expr, ast.Subscript):
        # Subscript: col['key'] (map or struct access) or col[0] (array access).
        base_type = _resolve_expr_type(expr.expr, scope)
        if isinstance(base_type, MapType):
            return base_type.value.type
        if isinstance(base_type, StructType) and isinstance(expr.index, ast.String):
            field_name = expr.index.value.strip("'\"")
            if field_name in base_type.fields_mapping:
                return base_type.fields_mapping[field_name].type
        return UnknownType()  # pragma: no cover

    if isinstance(expr, ast.Case):
        for case_result in expr.results:
            resolved = _resolve_expr_type(case_result, scope)
            if not isinstance(resolved, UnknownType):
                return resolved
        if expr.else_result:
            resolved = _resolve_expr_type(expr.else_result, scope)
            if not isinstance(resolved, UnknownType):  # pragma: no branch
                return resolved
        return UnknownType()

    # Fallback: try the expression's own type property  # pragma: no cover
    try:  # pragma: no cover
        result_type = getattr(expr, "type", None)  # pragma: no cover
        if isinstance(result_type, ColumnType):  # pragma: no cover
            return result_type  # pragma: no cover
    except Exception:  # pragma: no cover
        pass  # pragma: no cover
    return UnknownType()  # pragma: no cover


# ---------------------------------------------------------------------------
# Recursive type stamping
# ---------------------------------------------------------------------------


def _stamp_types_recursive(node: ast.Node, scope: "TypeScope"):
    """Set _type on all nodes in a subtree so the AST's dispatch chain works.

    The function dispatch mechanism calls arg.type which chains through
    Alias.type → child.type → Function.type → ... If any intermediate node
    doesn't have _type set, it tries to resolve via the uncompiled AST and
    fails. This function resolves types top-down using our scope and stamps
    _type on every node that has the attribute.
    """
    if isinstance(node, ast.Lambda):
        return
    if isinstance(node, (ast.Query, ast.Select)):  # pragma: no cover
        return

    # Resolve and stamp this node's type
    _type = getattr(node, "_type", _SENTINEL)
    if _type is not _SENTINEL and _type is None:
        try:
            node._type = _resolve_expr_type(node, scope)  # type: ignore[union-attr]
        except TypeResolutionError as exc:
            node._type = UnknownType()  # type: ignore[union-attr]
            scope.errors.append(str(exc))
        except Exception:  # pragma: no cover
            node._type = UnknownType()  # type: ignore[union-attr]

    # Recurse into children
    for child in node.children:
        _stamp_types_recursive(child, scope)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_output_name(expr: ast.Node) -> str:
    """Get the output column name for a projection expression."""
    if isinstance(expr, ast.Aliasable) and expr.alias:
        return expr.alias.name
    if isinstance(expr, (ast.Aliasable, ast.Named)):
        return expr.alias_or_name.name
    return str(expr)


def columns_signature_changed(
    old: OutputColumns,
    new: OutputColumns,
) -> bool:
    """
    Check if the column signature (names + types) changed between two versions.

    Used as a fast-path skip: if a parent's column signature didn't change,
    its downstream nodes' types can't have changed either.

    Any UnknownType in either old or new is treated as "changed" since we
    can't confirm compatibility.
    """
    if len(old) != len(new):
        return True
    for (old_name, old_type), (new_name, new_type) in zip(old, new):
        if old_name != new_name:
            return True
        if isinstance(old_type, UnknownType) or isinstance(new_type, UnknownType):
            return True
        if str(old_type) != str(new_type):
            return True
    return False
