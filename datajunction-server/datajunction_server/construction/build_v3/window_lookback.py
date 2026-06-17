from dataclasses import dataclass
from typing import Optional

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast

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
