from dataclasses import dataclass
from typing import Optional

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

ADDITIVE_WINDOW_AGGS = frozenset({"SUM", "COUNT"})


@dataclass
class WindowLookback:
    """Extent + order column read off a window-frame metric expression."""

    extent: int  # N from "N PRECEDING"
    order_column: ast.Column  # the OVER(ORDER BY <col>) column
    agg_name: str  # upper-cased window aggregation function name


def read_window_lookback(expr: ast.Function) -> Optional[WindowLookback]:
    """
    Read the lookback extent + order column from a window-frame function.
    Returns None if `expr` has no OVER clause with a row/range frame whose
    start is "<N> PRECEDING" over a single ORDER BY column.
    """
    over = getattr(expr, "over", None)
    if over is None or over.window_frame is None or not over.order_by:
        return None
    frame = over.window_frame
    if frame.start is None or frame.start.stop != "PRECEDING":
        return None
    try:
        extent = int(frame.start.start)
    except (TypeError, ValueError):
        return None
    order_expr = over.order_by[0].expr
    if not isinstance(order_expr, ast.Column):
        return None
    return WindowLookback(
        extent=extent,
        order_column=order_expr,
        agg_name=expr.name.name.upper(),
    )


def validate_window_lookback(wl: "WindowLookback", order_is_sequence_dim: bool) -> None:
    """Raise DJInvalidInputException if this window metric is unsupported in v1."""
    if not order_is_sequence_dim:
        raise DJInvalidInputException(
            message=(
                f"Window metric orders by '{wl.order_column.name.name}', which is not an "
                "orderable sequence dimension. Window metrics must order by an orderable "
                "sequence dimension (e.g. a date dimension)."
            ),
        )
    if wl.agg_name not in ADDITIVE_WINDOW_AGGS:
        raise DJInvalidInputException(
            message=(
                f"Window aggregation '{wl.agg_name}' is not supported on the live path. "
                "Only additive window aggregations (SUM, COUNT) are supported today; "
                "gap-fill semantics for averages are not yet defined."
            ),
        )


def build_densify_join(
    spine_table: ast.Table,
    spine_key: ast.Column,
    fact_key: ast.Column,
) -> ast.Join:
    """
    LEFT JOIN the driving fact to the sequence dimension's domain so every domain
    value gets a row. Emitted as a LEFT join whose ON equates the spine key to the
    fact's order key.
    """
    return ast.Join(
        join_type="LEFT",
        right=spine_table,
        criteria=ast.JoinCriteria(on=ast.BinaryOp.Eq(spine_key, fact_key)),
    )


def zero_fill(measure_expr: ast.Expression) -> ast.Function:
    """Wrap an additive measure in COALESCE(measure, 0) for densified gap rows."""
    return ast.Function(ast.Name("COALESCE"), args=[measure_expr, ast.Number(0)])


def build_scan_bounds(
    col_ref: ast.Expression,
    low_expr: ast.Expression,
    high_expr: ast.Expression,
    offset_low_expr: ast.Expression,
) -> ast.Between:
    """
    Scan filter for the densified series: BETWEEN (lower − N) AND upper.
    `offset_low_expr` is the value N positions before the requested lower,
    resolved by the caller against the sequence dimension's order.
    """
    return ast.Between(expr=col_ref, low=offset_low_expr, high=high_expr)


def build_output_restriction(
    col_ref: ast.Expression,
    low_expr: ast.Expression,
    high_expr: ast.Expression,
) -> ast.Between:
    """The originally-requested predicate, re-applied ABOVE the window so the
    lookback rows used to seed the frame do not leak into the result."""
    return ast.Between(expr=col_ref, low=low_expr, high=high_expr)


def resolve_offset_low(
    dim_table: ast.Table,
    order_col_name: str,
    lower_expr: ast.Expression,
    extent: int,
) -> ast.Expression:
    """
    Scalar expression for "the value `extent` positions before `lower`" in the
    sequence dimension's order, built as a ranked scalar subquery so it works for
    any orderable sequence dimension (not just arithmetic ones):

        (SELECT MIN(__o.<col>) FROM (
            SELECT <col> FROM <dim> WHERE <col> <= <lower>
            ORDER BY <col> DESC LIMIT <extent + 1>
         ) __o)
    """
    tbl = dim_table.name.name
    sql = (
        f"SELECT (SELECT MIN(__o.{order_col_name}) FROM ("
        f"SELECT {order_col_name} FROM {tbl} "
        f"WHERE {order_col_name} <= {str(lower_expr)} "
        f"ORDER BY {order_col_name} DESC LIMIT {extent + 1}"
        f") __o)"
    )
    return parse(sql).select.projection[0]
