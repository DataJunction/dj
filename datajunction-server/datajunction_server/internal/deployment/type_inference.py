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

from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing import ast
from datajunction_server.errors import DJNotImplementedException
from datajunction_server.sql.parsing.types import (
    BooleanType,
    ColumnType,
    NullType,
    StringType,
    UnknownType,
)

logger = logging.getLogger(__name__)

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


class TypeResolutionError(Exception):
    """Raised when type resolution fails (missing table, missing column, etc.)."""


def resolve_output_columns(
    query_str: str,
    parent_columns_map: ParentColumnsMap,
) -> OutputColumns:
    """
    Resolve output column names and types for a SQL query.

    Args:
        query_str: The SQL query to analyze.
        parent_columns_map: Pre-loaded map of parent node names to their
            column name→type mappings.

    Returns:
        List of (column_name, column_type) for each projection column.

    Raises:
        TypeResolutionError: If a referenced table or column cannot be resolved.
    """
    try:
        query = parse(query_str)
    except Exception as exc:
        raise TypeResolutionError(f"Failed to parse query: {exc}") from exc

    cte_registry: dict[str, OutputColumns] = {}
    for cte in query.ctes:
        cte_name = cte.alias_or_name.name
        cte_columns = _resolve_query(cte, parent_columns_map, cte_registry)
        cte_registry[cte_name] = cte_columns

    return _resolve_query(query, parent_columns_map, cte_registry)


def _resolve_query(
    query: ast.Query,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
) -> OutputColumns:
    """Resolve output columns for a single query (may be a subquery or CTE)."""
    select = query.select
    if isinstance(select, ast.InlineTable):
        return _resolve_inline_table(query)

    if not isinstance(select, ast.Select):  # pragma: no cover
        return []

    tables = _build_table_scope(select, parent_columns_map, cte_registry)
    scope = TypeScope(tables=tables, parent_map=parent_columns_map)

    output: OutputColumns = []
    for expr in select.projection:
        output.extend(_resolve_projection_expr(expr, scope))
    return output


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
) -> TableScope:
    """
    Build a mapping of table alias/name → {column_name: column_type} for all
    tables available in this query's scope.
    """
    if select.from_ is None:
        return {"__derived__": _build_derived_scope(parent_columns_map)}

    scope: TableScope = {}
    for relation in select.from_.relations:
        scope.update(
            _collect_tables_from_relation(relation, parent_columns_map, cte_registry),
        )
    for view in select.lateral_views:
        scope.update(_collect_lateral_view_columns(view))
    return scope


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
) -> TableScope:
    """Collect table scopes from a FROM relation. Returns discovered tables."""
    result: TableScope = {}

    if isinstance(node, ast.Relation):
        result.update(
            _collect_tables_from_relation(
                node.primary,
                parent_columns_map,
                cte_registry,
            ),
        )
        for ext in node.extensions:
            if isinstance(ext, ast.Join):  # pragma: no branch
                result.update(
                    _collect_tables_from_relation(
                        ext.right,
                        parent_columns_map,
                        cte_registry,
                    ),
                )

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
        sub_columns = _resolve_query(node, parent_columns_map, cte_registry)
        result[sub_alias] = {name: typ for name, typ in sub_columns}

    elif isinstance(node, ast.FunctionTableExpression):  # pragma: no branch
        alias = node.alias.name if node.alias else "__func_table__"
        func_cols = {col.name.name: UnknownType() for col in (node.column_list or [])}
        if func_cols:  # pragma: no branch
            result[alias] = func_cols

    return result


def _collect_lateral_view_columns(view: ast.LateralView) -> TableScope:
    """
    Collect columns from a LATERAL VIEW (e.g., EXPLODE) expression.
    Returns {alias: {col_name: type}} for the exploded columns.
    """
    func = view.func
    alias = func.alias.name if func.alias else "__lateral__"
    lateral_cols = {col.name.name: UnknownType() for col in (func.column_list or [])}
    if lateral_cols:
        return {alias: lateral_cols}
    return {}


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
            raise TypeResolutionError(
                f"Column `{col_name}` not found in table `{table_alias}`. "
                f"Available: {list(scope.tables[table_alias].keys())}",
            )

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
    resolve_output_columns call.
    """
    if isinstance(expr, ast.Column):
        return _resolve_column_type(expr, scope)

    if isinstance(expr, ast.Function):
        try:
            _prepare_function_arg_types(expr, scope)
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
    if hasattr(expr, "type"):  # pragma: no cover
        result_type = expr.type  # type: ignore[attr-defined]  # pragma: no cover
        if isinstance(result_type, ColumnType):  # pragma: no cover
            return result_type  # pragma: no cover
    return UnknownType()  # pragma: no cover


# ---------------------------------------------------------------------------
# Function argument type resolution
# ---------------------------------------------------------------------------


def _prepare_function_arg_types(
    func: ast.Function,
    scope: TypeScope,
):
    """
    Set _type on function argument AST nodes so Function.infer_type can dispatch.

    Mutates the AST in place. This is intentional - the AST is a throwaway object
    created per resolve_output_columns call and discarded after.
    """
    for arg in func.args:
        if isinstance(arg, ast.Column) and arg._type is None:
            try:
                arg._type = _resolve_column_type(arg, scope)
            except TypeResolutionError:
                arg._type = UnknownType()
        elif isinstance(arg, ast.Function):
            _prepare_function_arg_types(arg, scope)
        elif isinstance(arg, (ast.Wildcard, ast.Number, ast.String)):
            pass
        elif isinstance(arg, ast.Expression):  # pragma: no branch
            _prepare_column_types_recursive(arg, scope)


def _prepare_column_types_recursive(
    node: ast.Node,
    scope: TypeScope,
):
    """Set _type on all unresolved Column nodes in an AST subtree.

    Mutates the AST in place so that expression .type properties (e.g., Case.type)
    can resolve without hitting DJParseException. Same mutation contract as
    _prepare_function_arg_types.
    """
    if isinstance(node, ast.Column) and node._type is None:
        try:
            node._type = _resolve_column_type(node, scope)
        except TypeResolutionError:
            node._type = UnknownType()
    if isinstance(node, ast.Function):
        _prepare_function_arg_types(node, scope)
        return
    for child in node.children:
        _prepare_column_types_recursive(child, scope)


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
