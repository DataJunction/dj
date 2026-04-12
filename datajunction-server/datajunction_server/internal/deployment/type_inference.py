"""
Lightweight top-down type inference for deployment propagation.

This module resolves output column names and types for a SQL query using
pre-loaded parent column data. No DB calls — all resolution is in-memory.

Used by propagate_impact to cheaply revalidate downstream nodes when an
upstream node's columns change, without going through the full Query.compile
pipeline.
"""

import logging
from typing import Optional

from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import ColumnType, StringType, UnknownType

logger = logging.getLogger(__name__)

# Type alias: maps node name → {column_name: ColumnType}
ParentColumnsMap = dict[str, dict[str, ColumnType]]

# Output: list of (column_name, column_type) for the query's projection
OutputColumns = list[tuple[str, ColumnType]]


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

    # Build a table registry from CTEs and FROM tables
    cte_registry: dict[str, OutputColumns] = {}

    # Resolve CTEs first (they can reference parent tables and earlier CTEs)
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
    """
    Resolve output columns for a single query (may be a subquery or CTE).
    """
    select = query.select
    if isinstance(select, ast.InlineTable):
        return _resolve_inline_table(query)

    if not isinstance(select, ast.Select):
        return []

    # Step 1: Build the table scope — maps alias/name → {col_name: type}
    table_scope = _build_table_scope(select, parent_columns_map, cte_registry)

    # Step 2: Resolve each projection expression
    output: OutputColumns = []
    for expr in select.projection:
        resolved = _resolve_projection_expr(expr, table_scope)
        output.extend(resolved)
    return output


def _resolve_inline_table(query: ast.Query) -> OutputColumns:
    """Resolve columns for a VALUES expression with explicit column aliases."""
    if query.column_list:
        return [(col.alias_or_name.name, UnknownType()) for col in query.column_list]
    select = query.select
    if isinstance(select, ast.InlineTable) and select._columns:
        return [(col.alias_or_name.name, UnknownType()) for col in select._columns]
    return []


def _build_table_scope(
    select: ast.Select,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
) -> dict[str, dict[str, ColumnType]]:
    """
    Build a mapping of table alias/name → {column_name: column_type} for all
    tables in the FROM clause.
    """
    scope: dict[str, dict[str, ColumnType]] = {}

    if select.from_ is None:
        # No FROM clause — derived metric pattern.
        # Return parent columns indexed by node name so column resolution
        # can look up metric references.
        return {"__derived__": _build_derived_scope(parent_columns_map)}

    for relation in select.from_.relations:
        _collect_tables_from_relation(
            relation,
            parent_columns_map,
            cte_registry,
            scope,
        )

    # Process LATERAL VIEW clauses — they add columns from table-valued functions
    # (e.g., EXPLODE, UNNEST) into the scope.
    for view in select.lateral_views:
        _collect_lateral_view_columns(view, scope)

    return scope


def _build_derived_scope(parent_columns_map: ParentColumnsMap) -> dict[str, ColumnType]:
    """
    For derived metrics (no FROM clause), build a flat scope that maps
    metric node names to their output types.
    """
    scope: dict[str, ColumnType] = {}
    for node_name, columns in parent_columns_map.items():
        # Metric nodes have a single output column — map the node name to its type
        if len(columns) == 1:
            col_type = next(iter(columns.values()))
            scope[node_name] = col_type
        # Also add individual columns for dimension attribute access
        for col_name, col_type in columns.items():
            scope[f"{node_name}.{col_name}"] = col_type
    return scope


def _collect_tables_from_relation(
    node: ast.Node,
    parent_columns_map: ParentColumnsMap,
    cte_registry: dict[str, OutputColumns],
    scope: dict[str, dict[str, ColumnType]],
):
    """Recursively collect all table sources from a FROM relation."""
    if isinstance(node, ast.Relation):
        _collect_tables_from_relation(
            node.primary,
            parent_columns_map,
            cte_registry,
            scope,
        )
        for ext in node.extensions:
            if isinstance(ext, ast.Join):
                _collect_tables_from_relation(
                    ext.right,
                    parent_columns_map,
                    cte_registry,
                    scope,
                )
    elif isinstance(node, ast.Table):
        table_name = node.identifier(quotes=False)
        alias = node.alias.name if node.alias else table_name

        # Check CTEs first
        if table_name in cte_registry:
            scope[alias] = {name: typ for name, typ in cte_registry[table_name]}
            return

        # Then check parent columns map
        if table_name in parent_columns_map:
            scope[alias] = dict(parent_columns_map[table_name])
            return

        # Table not found
        raise TypeResolutionError(
            f"Table `{table_name}` not found in parent columns map. "
            f"Available: {list(parent_columns_map.keys())}",
        )
    elif isinstance(node, ast.Query):
        # Inline subquery: resolve it recursively
        sub_alias = node.alias.name if node.alias else "__subquery__"
        sub_columns = _resolve_query(node, parent_columns_map, cte_registry)
        scope[sub_alias] = {name: typ for name, typ in sub_columns}
    elif isinstance(node, ast.FunctionTableExpression):
        # Table-valued function (CROSS JOIN UNNEST(...) t(col1, col2))
        alias = node.alias.name if node.alias else "__func_table__"
        func_cols: dict[str, ColumnType] = {}
        if node.column_list:
            for col in node.column_list:
                func_cols[col.name.name] = UnknownType()
        if func_cols:
            scope[alias] = func_cols


def _collect_lateral_view_columns(
    view: ast.LateralView,
    scope: dict[str, dict[str, ColumnType]],
):
    """
    Add columns from a LATERAL VIEW (e.g., EXPLODE) into the table scope.

    LATERAL VIEW EXPLODE(col) alias AS col_name — adds col_name to scope.
    The alias becomes the "table" name, and column_list items become available columns.
    """
    func = view.func
    alias = func.alias.name if func.alias else "__lateral__"

    lateral_cols: dict[str, ColumnType] = {}
    if func.column_list:
        for col in func.column_list:
            # We don't know the exact exploded type without resolving the function,
            # so use StringType as a safe default. The important thing is that the
            # column name is in scope so resolution doesn't fail.
            lateral_cols[col.name.name] = UnknownType()

    if lateral_cols:
        scope[alias] = lateral_cols


def _resolve_projection_expr(
    expr: ast.Node,
    table_scope: dict[str, dict[str, ColumnType]],
) -> OutputColumns:
    """
    Resolve a single projection expression to its output column(s).

    Handles: Column refs, Wildcard, aliases, functions, literals, expressions.
    """
    if isinstance(expr, ast.Wildcard):
        return _resolve_wildcard(None, table_scope)

    # Unwrap Alias(child=..., alias="name") → resolve the child, use the alias as name
    if isinstance(expr, ast.Alias):
        output_name = expr.alias.name if expr.alias else _get_output_name(expr.child)
        child_results = _resolve_projection_expr(expr.child, table_scope)
        if child_results:
            # Replace the name with the alias
            return [(output_name, child_results[0][1])]
        return [(output_name, UnknownType())]

    output_name = _get_output_name(expr)

    if isinstance(expr, ast.Column):
        # Check for table-qualified wildcard: t.*
        if isinstance(expr.expression, ast.Wildcard) or (
            expr.name and expr.name.name == "*"
        ):
            table_alias = expr.namespace[0].name if expr.namespace else None
            return _resolve_wildcard(table_alias, table_scope)

        # Column wrapping an expression (e.g., SUM(amount) AS total)
        if expr.expression is not None:
            col_type = _resolve_expr_type(expr.expression, table_scope)
            return [(output_name, col_type)]

        col_type = _resolve_column_type(expr, table_scope)
        return [(output_name, col_type)]

    # For expressions (Function, BinaryOp, Cast, literals, etc.), try to infer type
    col_type = _resolve_expr_type(expr, table_scope)
    return [(output_name, col_type)]


def _resolve_wildcard(
    table_alias: Optional[str],
    table_scope: dict[str, dict[str, ColumnType]],
) -> OutputColumns:
    """Expand * or t.* into all columns from the relevant table(s)."""
    if table_alias:
        if table_alias not in table_scope:
            raise TypeResolutionError(
                f"Table alias `{table_alias}` not found for wildcard expansion.",
            )
        return list(table_scope[table_alias].items())

    # Unqualified * — all columns from all tables
    result: OutputColumns = []
    for cols in table_scope.values():
        result.extend(cols.items())
    return result


def _resolve_column_type(
    col: ast.Column,
    table_scope: dict[str, dict[str, ColumnType]],
) -> ColumnType:
    """Resolve a column reference to its type using the table scope."""
    # Use the actual column name (not alias) for lookup
    col_name = col.name.name

    # Derived metric pattern (no FROM clause)
    if "__derived__" in table_scope:
        derived = table_scope["__derived__"]
        # Try full identifier (e.g., "default.total_revenue")
        full_id = col.identifier()
        if full_id in derived:
            return derived[full_id]
        # Try as dimension attribute: "default.dim.column"
        if col.namespace:
            ns_parts = [n.name for n in col.namespace]
            dim_name = ".".join(ns_parts)
            attr_key = f"{dim_name}.{col_name}"
            if attr_key in derived:
                return derived[attr_key]
        raise TypeResolutionError(
            f"Column `{col}` not found in derived metric scope.",
        )

    # Table-qualified column: namespace.column_name
    if col.namespace:
        table_alias = col.namespace[0].name
        if table_alias in table_scope:
            if col_name in table_scope[table_alias]:
                return table_scope[table_alias][col_name]
            raise TypeResolutionError(
                f"Column `{col_name}` not found in table `{table_alias}`. "
                f"Available: {list(table_scope[table_alias].keys())}",
            )

    # Unqualified column — search all tables
    found_type = None
    found_in = None
    for alias, cols in table_scope.items():
        if col_name in cols:
            if found_type is not None:
                raise TypeResolutionError(
                    f"Column `{col_name}` is ambiguous — found in "
                    f"`{found_in}` and `{alias}`.",
                )
            found_type = cols[col_name]
            found_in = alias

    if found_type is not None:
        return found_type

    raise TypeResolutionError(
        f"Column `{col_name}` not found in any table. "
        f"Available tables: {list(table_scope.keys())}",
    )


def _resolve_expr_type(
    expr: ast.Node,
    table_scope: dict[str, dict[str, ColumnType]],
) -> ColumnType:
    """
    Resolve the type of an arbitrary expression.

    Delegates to the AST's own type inference where possible (Function.type,
    Cast, literals). For column references, uses the table scope.
    """
    if isinstance(expr, ast.Column):
        return _resolve_column_type(expr, table_scope)

    if isinstance(expr, ast.Function):
        # Resolve arg types first so Function.infer_type can work
        _resolve_function_arg_types(expr, table_scope)
        try:
            result = expr.type
            if result is not None:
                return result
        except Exception as exc:
            logger.info(
                "Function type inference failed for %s: %s. Arg types: %s",
                expr.name,
                exc,
                [
                    (type(a).__name__, getattr(a, "_type", "no _type"))
                    for a in expr.args
                ],
            )
        return UnknownType()  # Fallback for unresolvable function types

    if isinstance(expr, ast.Cast):
        # CAST(x AS type) — type is explicit
        return expr.data_type

    if isinstance(expr, ast.Number):
        try:
            return expr.type
        except Exception:
            from datajunction_server.sql.parsing.types import IntegerType

            return IntegerType()

    if isinstance(expr, ast.String):
        return StringType()

    if isinstance(expr, ast.Boolean):
        from datajunction_server.sql.parsing.types import BooleanType

        return BooleanType()

    if isinstance(expr, ast.Null):
        from datajunction_server.sql.parsing.types import NullType

        return NullType()

    # Binary operations: try to resolve via the AST's type property
    if isinstance(expr, ast.BinaryOp):
        # Resolve operand types first
        _resolve_expr_type(expr.left, table_scope)
        _resolve_expr_type(expr.right, table_scope)
        try:
            return expr.type
        except Exception:
            # If type can't be inferred, try using left operand's type
            try:
                return _resolve_expr_type(expr.left, table_scope)
            except Exception:
                return UnknownType()

    # Case expressions
    if isinstance(expr, ast.Case):
        for case_result in expr.results:
            try:
                resolved = _resolve_expr_type(case_result, table_scope)
                return resolved
            except Exception:
                continue
        if expr.else_result:
            return _resolve_expr_type(expr.else_result, table_scope)
        return UnknownType()

    # Fallback: try the expression's own type property
    try:
        if hasattr(expr, "type"):
            result_type = expr.type  # type: ignore[attr-defined]
            if isinstance(result_type, ColumnType):
                return result_type
        return UnknownType()
    except Exception:
        return UnknownType()


def _resolve_function_arg_types(
    func: ast.Function,
    table_scope: dict[str, dict[str, ColumnType]],
):
    """Pre-resolve column types in function arguments so type inference works."""
    for arg in func.args:
        if isinstance(arg, ast.Column) and arg._type is None:
            try:
                resolved = _resolve_column_type(arg, table_scope)
                arg._type = resolved
            except TypeResolutionError:
                pass  # Some args like * or literals don't need resolution
        elif isinstance(arg, ast.Function):
            _resolve_function_arg_types(arg, table_scope)
        elif isinstance(arg, (ast.Wildcard, ast.Number, ast.String)):
            pass  # These have their own type properties
        elif isinstance(arg, ast.Expression):
            # Resolve nested expressions (CASE, CAST, BinaryOp, etc.)
            # First, recursively resolve all Column types within this expression
            # so that the expression's own .type property can work.
            _set_column_types_recursive(arg, table_scope)
            try:
                resolved = _resolve_expr_type(arg, table_scope)
                if hasattr(arg, "_type"):
                    arg._type = resolved
            except (TypeResolutionError, Exception):
                pass


def _set_column_types_recursive(
    node: ast.Node,
    table_scope: dict[str, dict[str, ColumnType]],
):
    """
    Walk an AST subtree and set _type on all Column nodes that don't have
    one yet. This ensures that expression .type properties (e.g., Case.type)
    can resolve without hitting DJParseException.
    """
    if isinstance(node, ast.Column) and node._type is None:
        try:
            node._type = _resolve_column_type(node, table_scope)
        except TypeResolutionError:
            pass
    if isinstance(node, ast.Function):
        _resolve_function_arg_types(node, table_scope)
        return  # _resolve_function_arg_types handles recursion for function args
    for child in node.children:
        _set_column_types_recursive(child, table_scope)


def _get_output_name(expr: ast.Node) -> str:
    """Get the output column name for a projection expression."""
    if isinstance(expr, ast.Aliasable) and expr.alias:
        return expr.alias.name
    if isinstance(expr, (ast.Aliasable, ast.Named)):
        try:
            return expr.alias_or_name.name
        except Exception:
            pass
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
