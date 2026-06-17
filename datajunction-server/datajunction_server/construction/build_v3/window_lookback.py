from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse

if TYPE_CHECKING:
    from datajunction_server.construction.build_v3.types import BuildContext

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


def _read_lookback_role_aware(
    func: ast.Function,
) -> Optional[tuple["WindowLookback", Optional[str]]]:
    """
    Wrap :func:`read_window_lookback` so a role-qualified order column
    (``ORDER BY v3.date.date_id[order]``, parsed as a ``Subscript`` over a
    ``Column``) is read correctly, returning the resolved ``WindowLookback``
    plus the role string (``"order"``) when present.

    The contract :func:`read_window_lookback` only recognizes a bare
    ``ast.Column`` order expression; the live query path routinely carries a
    role subscript, so this adapter normalizes that shape without changing the
    contract.
    """
    over = getattr(func, "over", None)
    if over is None or not over.order_by:
        return None
    order_expr = over.order_by[0].expr

    role: Optional[str] = None
    if isinstance(order_expr, ast.Subscript) and isinstance(
        order_expr.expr,
        ast.Column,
    ):
        # Re-parent the inner Column onto a temporary copy of the function so
        # read_window_lookback sees a bare Column, then restore.
        role = str(order_expr.index)
        original = over.order_by[0].expr
        over.order_by[0].expr = order_expr.expr
        try:
            wl = read_window_lookback(func)
        finally:
            over.order_by[0].expr = original
        return (wl, role) if wl else None

    wl = read_window_lookback(func)
    return (wl, None) if wl else None


def _order_filter_bounds(
    filter_ast: ast.Expression,
    order_col_name: str,
) -> Optional[tuple[Optional[ast.Expression], Optional[ast.Expression]]]:
    """
    Read (low, high) bounds off a dimension filter that constrains the order
    column, or None if this filter does not constrain it.

    Supports the shapes the live path needs to expand:
      - ``col = R``             -> (R, R)            (a self-contained point)
      - ``col BETWEEN A AND B`` -> (A, B)            (a self-contained range)
      - ``col >= A`` / ``col > A`` -> (A, None)      (a low-only partial bound)
      - ``col <= B`` / ``col < B`` -> (None, B)      (a high-only partial bound)

    Partial bounds (one side ``None``) let the caller stitch a range expressed
    as two separate filter strings (``col >= A`` and ``col <= B``) into a single
    ``[A, B]`` extent. The caller is responsible for combining them.
    """
    cols = [
        c
        for c in filter_ast.find_all(ast.Column)
        if c.name.name == order_col_name
    ]
    if not cols:
        return None

    if isinstance(filter_ast, ast.Between):
        return filter_ast.low, filter_ast.high

    if isinstance(filter_ast, ast.BinaryOp):
        left_is_col = (
            isinstance(filter_ast.left, ast.Column)
            and filter_ast.left.name.name == order_col_name
        )
        # The value sits on whichever side is not the order column.
        value = filter_ast.right if left_is_col else filter_ast.left
        op = filter_ast.op

        if op == ast.BinaryOpKind.Eq:
            return value, value

        # Normalize the comparison so it reads as ``col <op> value``: if the
        # column is on the right (``value <op> col``), flip the direction.
        if op in (ast.BinaryOpKind.GtEq, ast.BinaryOpKind.Gt):
            return (value, None) if left_is_col else (None, value)
        if op in (ast.BinaryOpKind.LtEq, ast.BinaryOpKind.Lt):
            return (None, value) if left_is_col else (value, None)

    return None


def _is_static_literal_bound(expr: Optional[ast.Expression]) -> bool:
    """
    True when ``expr`` is a static literal we can safely interpolate into the
    offset subquery (see :func:`resolve_offset_low`, which renders the lower
    bound via ``str(...)``) and reason about for reconciliation.

    Only numbers and strings qualify. A non-literal bound (``func(col) = R``, a
    column reference, an arithmetic expression, etc.) must NOT be stitched: we
    cannot statically compare it against another bound nor splice it into the
    ranked offset subquery, so the caller bails on expansion and leaves the
    user's filters untouched.
    """
    return isinstance(expr, (ast.Number, ast.String))


def _tighter_bound(
    current: Optional[ast.Expression],
    candidate: ast.Expression,
    *,
    keep_max: bool,
) -> Optional[ast.Expression]:
    """
    Reconcile two same-side bounds to the TIGHTEST one. For a lower bound keep
    the MAX (``keep_max=True``); for an upper bound keep the MIN. Comparison is
    only defined for two ``ast.Number`` literals (compared on ``.value``).

    Returns the retained bound, or ``None`` to signal "cannot compare
    statically -> bail" when either side is not a comparable numeric literal.
    The first observed bound (``current is None``) is accepted as-is, but only
    if it is itself a comparable literal.
    """
    if current is None:
        return candidate if isinstance(candidate, ast.Number) else None
    if not isinstance(current, ast.Number) or not isinstance(candidate, ast.Number):
        return None
    if keep_max:
        return current if current.value >= candidate.value else candidate
    return current if current.value <= candidate.value else candidate


def _dimension_physical_table(
    ctx: "BuildContext",
    dim_node,
) -> Optional[str]:
    """
    Resolve the physical (``catalog.schema.table``) source for a sequence
    dimension so the offset subquery can reference it directly.

    Returns the dimension's own physical table when it is itself a source, or
    the physical table of the single source its query reads from. Returns None
    when the dimension's domain cannot be reduced to one physical table.
    """
    from datajunction_server.construction.build_v3.cte import (
        get_table_references_from_ast,
    )
    from datajunction_server.construction.build_v3.materialization import (
        get_physical_table_name,
    )
    from datajunction_server.models.node import NodeType as _NT

    if dim_node.type == _NT.SOURCE:  # pragma: no cover
        return get_physical_table_name(dim_node)

    if not dim_node.current or not dim_node.current.query:  # pragma: no cover
        return None

    query_ast = ctx.get_parsed_query(dim_node)
    refs = get_table_references_from_ast(query_ast)
    source_tables = []
    for ref in refs:
        ref_node = ctx.nodes.get(ref)
        if ref_node and ref_node.type == _NT.SOURCE:
            physical = get_physical_table_name(ref_node)
            if physical:  # pragma: no branch
                source_tables.append(physical)
    if len(source_tables) == 1:
        return source_tables[0]
    return None  # pragma: no cover


def apply_live_window_lookback(ctx: "BuildContext") -> None:
    """
    Live frame-aware lookback adapter (mirror of the cube-side
    :func:`build_temporal_filter`).

    When a requested metric carries a row/range window frame
    (``... OVER (ORDER BY <date> ROWS BETWEEN N PRECEDING AND CURRENT ROW)``)
    *and* the query is filtered to a narrow range on that order dimension, a
    naive build pushes the narrow predicate into the scan — starving the frame
    of its N preceding rows and producing wrong results.

    This adapter rewrites the build so that:

    1. The *scan* is expanded to ``[R - N, R]``: a
       ``fk_col BETWEEN <offset_low> AND <high>`` predicate (``offset_low`` =
       the value N positions before the requested low, resolved against the
       order dimension via :func:`resolve_offset_low`) is injected directly
       into the fact's scan CTE through ``ctx.upstream_pushdown_filters``. The
       original narrow predicate is marked consumed so neither the scan nor the
       windowed query re-applies it.
    2. The originally-requested predicate is recorded on ``ctx`` so it can be
       re-applied ABOVE the window (in a wrapper SELECT) — never in the
       windowed query's own WHERE, which SQL evaluates *before* the window and
       would re-starve the frame.

    The requested range may arrive as a single self-contained filter
    (``= R`` or ``BETWEEN A AND B``) or as several one-sided filters
    (``>= A`` and ``<= B``) on the same order column. Multiple bounds on the
    same side are reconciled to the tightest (max of lows, min of highs).

    Several conditions cause this adapter to **bail** for an order column —
    leaving every one of its filters intact and *un-consumed* so the standard
    pushdown re-enforces them (the window may then be starved, the documented
    safe fallback, rather than silently dropping a constraint):

    - any matching bound on the order column is a non-literal expression
      (it must not be interpolated into the offset subquery), or
    - two same-side bounds are not both statically comparable literals.

    The invariant: a filter is consumed only when its constraint is guaranteed
    to be re-applied by the output restriction. All ``ctx`` mutation is deferred
    until after the bail/validation gates so a bail never leaves partial state.

    No-op when no requested metric has a window frame, or when no filter
    constrains the frame's order column.
    """
    from datajunction_server.construction.build_v3.cte import has_window_function
    from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref
    from datajunction_server.construction.build_v3.filters import parse_filter
    from datajunction_server.construction.build_v3.utils import make_column_ref

    # Collect window-frame lookbacks across all decomposed metrics, keyed by the
    # order dimension ref. Multiple frames on the same order column collapse to
    # the maximum extent (the widest scan that satisfies every frame).
    max_extent_by_order: dict[str, int] = {}
    sample_wl_by_order: dict[str, WindowLookback] = {}
    for decomposed in ctx.decomposed_metrics.values():
        combiner = decomposed.combiner_ast
        if not has_window_function(combiner):
            continue
        for func in combiner.find_all(ast.Function):
            if not func.over:
                continue
            read = _read_lookback_role_aware(func)
            if read is None:
                continue
            wl, role = read
            # Build the order dimension ref (node.column[role]) so downstream
            # resolution matches the user's filter/dimension reference.
            order_ref = str(wl.order_column)
            if role:
                order_ref = f"{order_ref}[{role}]"
            max_extent_by_order[order_ref] = max(
                max_extent_by_order.get(order_ref, 0),
                wl.extent,
            )
            sample_wl_by_order.setdefault(order_ref, wl)

    if not max_extent_by_order:
        return

    for order_ref, extent in max_extent_by_order.items():
        parsed_order = parse_dimension_ref(order_ref)
        order_node_name = parsed_order.node_name
        order_col_name = parsed_order.column_name
        wl = sample_wl_by_order[order_ref]

        # Validate: the order column must resolve to a loaded dimension node
        # (an orderable sequence dimension) and the agg must be additive.
        order_node = ctx.nodes.get(order_node_name)
        order_is_sequence_dim = (
            order_node is not None
            and order_node.type == NodeType.DIMENSION
        )
        validate_window_lookback(wl, order_is_sequence_dim)

        # Find the dimension filter(s) constraining the order column. We match
        # on the *column* name since the user expresses the filter on the
        # dimension ref (e.g. "v3.date.date_id = 20240131"). A range may be
        # expressed as TWO separate filter strings (``col >= A`` and
        # ``col <= B``); we collect partial bounds across all matching filters
        # and stitch them into a single ``[low, high]`` extent.
        #
        # Reconcile (rather than last-write-wins) across every matching filter:
        # keep the TIGHTEST lower bound (MAX) and the TIGHTEST upper bound (MIN).
        # If any bound is a non-literal expression, or two same-side literal
        # bounds cannot be statically compared, BAIL on expansion for this order
        # column -- leaving ALL its filters intact and un-consumed so the normal
        # filter pushdown still enforces every constraint. We never pop a filter
        # whose constraint we are not going to re-apply above the window.
        target_indices: list[int] = []
        low_expr: Optional[ast.Expression] = None
        high_expr: Optional[ast.Expression] = None
        bail = False
        for idx, filter_str in enumerate(ctx.dimension_filters):
            filter_ast = parse_filter(filter_str)
            found = _order_filter_bounds(filter_ast, order_col_name)
            if found is None:
                continue
            found_low, found_high = found
            # Any non-literal bound on the order column poisons expansion: we
            # cannot interpolate it into the offset subquery nor compare it.
            if (found_low is not None and not _is_static_literal_bound(found_low)) or (
                found_high is not None and not _is_static_literal_bound(found_high)
            ):
                bail = True
                break
            if found_low is not None:
                reconciled = _tighter_bound(low_expr, found_low, keep_max=True)
                if reconciled is None:
                    bail = True
                    break
                low_expr = reconciled
            if found_high is not None:
                reconciled = _tighter_bound(high_expr, found_high, keep_max=False)
                if reconciled is None:
                    bail = True
                    break
                high_expr = reconciled
            target_indices.append(idx)

        if bail:
            # Leave every filter on this order column intact and un-consumed.
            continue

        if not target_indices or low_expr is None or high_expr is None:
            # No fully-bounded predicate on the order column -> nothing to
            # expand; the unbounded scan already feeds the frame correctly.
            # (A lone one-sided predicate, e.g. only ``col >= A``, has no upper
            # bound to anchor the expansion and is left untouched.)
            continue

        # Resolve the order dimension's physical source table so the offset
        # subquery is self-contained (mirrors build_temporal_filter referencing
        # the dimension's table directly rather than depending on CTE emission
        # order -- the dimension's own CTE is typically elided when the request
        # resolves the order column to the fact's join key).
        assert order_node is not None  # validated above
        dim_physical = _dimension_physical_table(ctx, order_node)
        if dim_physical is None:
            continue  # pragma: no cover
        dim_table = ast.Table(name=ast.Name(dim_physical))

        offset_low = resolve_offset_low(
            dim_table,
            order_col_name,
            low_expr,
            extent,
        )

        # Map the order dimension to each fact's foreign-key column and the
        # scan node that exposes it (mirror build_temporal_filter). The expanded
        # scan predicate is injected directly into that scan CTE as raw AST via
        # ctx.upstream_pushdown_filters -- NOT as a dimension-ref filter string,
        # because the offset subquery's internal columns (e.g. __o.<col>) are not
        # routable by the dimension-filter resolver.
        injected_any = False
        for metric_group in ctx.metric_groups:
            fact_node = metric_group.parent_node
            if not fact_node.current or not fact_node.current.dimension_links:
                continue  # pragma: no cover
            for link in fact_node.current.dimension_links:
                if link.dimension.name != order_node_name:
                    continue
                fk_columns = link.foreign_key_column_names
                if not fk_columns:
                    continue  # pragma: no cover
                fk_col_name = next(iter(fk_columns))

                scan_filter = build_scan_bounds(
                    make_column_ref(fk_col_name),
                    high_expr,
                    offset_low,
                )
                ctx.upstream_pushdown_filters.setdefault(
                    fact_node.name,
                    [],
                ).append(scan_filter)
                injected_any = True

        if not injected_any:
            # Could not reach the order dimension from any fact -> leave the
            # original filter in place (no expansion, no restriction).
            continue

        # The original narrow predicate(s) must NOT be pushed into the scan or
        # the windowed query's WHERE (both would re-starve the frame). Remove
        # them from the dimension filters and mark them consumed so the measures
        # layer skips them. Pop in descending index order so earlier indices stay
        # valid as we mutate the list.
        for idx in sorted(target_indices, reverse=True):
            consumed = ctx.dimension_filters.pop(idx)
            ctx.pushdown_consumed_filters.add(consumed)

        # Record the original predicate to be applied ABOVE the window. The
        # windowed projection aliases a role-qualified order dimension as
        # ``<column>_<role>`` (e.g. ``date_id_order``); plain dimensions keep
        # the bare column name. The wrapper re-qualifies this to the inner
        # subquery alias.
        output_col_name = (
            f"{order_col_name}_{parsed_order.role}"
            if parsed_order.role
            else order_col_name
        )
        output_restriction = build_output_restriction(
            make_column_ref(output_col_name),
            low_expr,
            high_expr,
        )
        ctx.live_window_output_restrictions.append(
            (output_col_name, output_restriction),
        )

        # Record a densification spec so the per-date grain relation the window
        # reads (the ``base_metrics`` CTE) can be rewritten into a DENSE series:
        # the existing per-date aggregation LEFT-joined onto the sequence
        # dimension's complete date domain over [offset_low, high], with additive
        # measures 0-filled on gap rows. Without this, ``ROWS BETWEEN N PRECEDING``
        # counts physical fact rows, so a sparse fact gives wrong trailing values.
        ctx.live_window_densify_specs.append(
            (output_col_name, dim_physical, order_col_name, offset_low, high_expr),
        )


def densify_window_base_metrics(
    result: "ast.Query",
    ctx: "BuildContext",
) -> None:
    """
    Rewrite the ``base_metrics`` CTE (the per-date grain relation the live
    window reads from) into a DENSE series so ``ROWS BETWEEN N PRECEDING``
    counts calendar positions rather than physical fact rows.

    For each densification spec recorded by :func:`apply_live_window_lookback`,
    the existing ``base_metrics`` SELECT (the per-date aggregation) is wrapped as
    an inner ``agg`` subquery and LEFT-joined onto a spine -- the sequence
    dimension's complete date domain over the expanded scan range
    ``[offset_low, high]``. The order dimension is projected from the spine (so
    gap dates appear), and every additive measure is wrapped in
    ``COALESCE(measure, 0)`` so missing dates contribute 0.

    Resulting CTE shape::

        base_metrics AS (
          SELECT spine.<key> AS <order_col>, COALESCE(agg.<m>, 0) AS <m>, ...
          FROM (SELECT DISTINCT <key> FROM <dim>
                WHERE <key> BETWEEN <offset_low> AND <high>) spine
          LEFT JOIN (<original base_metrics select>) agg
                 ON spine.<key> = agg.<order_col>
        )

    No-op when no densification spec was registered.
    """
    if not ctx.live_window_densify_specs:
        return

    # Locate the base_metrics CTE by alias.
    base_metrics_cte: Optional[ast.Query] = None
    for cte in result.ctes:
        if cte.alias is not None and cte.alias.name == "base_metrics":
            base_metrics_cte = cte
            break
    if base_metrics_cte is None:  # pragma: no cover
        return

    spine_alias = "__spine"
    agg_alias = "__agg"

    for (
        output_col_name,
        spine_physical,
        spine_key_col,
        offset_low,
        high_expr,
    ) in ctx.live_window_densify_specs:
        original_select = base_metrics_cte.select

        # The inner per-date aggregation becomes the right side of the LEFT join,
        # aliased ``__agg``.  Detach it from its CTE parent first so re-parenting
        # is clean.
        agg_query = ast.Query(select=original_select, ctes=[])
        agg_query.parenthesized = True
        agg_query.alias = ast.Name(agg_alias)
        agg_query.as_ = True

        # The spine: DISTINCT domain values of the sequence dimension over the
        # expanded scan range.  Built with the dimension's physical table so it
        # is self-contained (mirrors resolve_offset_low / the scan injection).
        spine_query = parse(
            f"SELECT DISTINCT {spine_key_col} FROM {spine_physical}",
        ).select
        spine_query.where = build_scan_bounds(
            ast.Column(name=ast.Name(spine_key_col)),
            high_expr,
            offset_low,
        )
        spine_subquery = ast.Query(select=spine_query, ctes=[])
        spine_subquery.parenthesized = True
        spine_subquery.alias = ast.Name(spine_alias)
        spine_subquery.as_ = True

        # LEFT JOIN agg onto the spine on the order column (reuses
        # build_densify_join): spine.<key> = agg.<order_col>.
        spine_key = ast.Column(
            name=ast.Name(spine_key_col),
            _table=ast.Table(name=ast.Name(spine_alias)),
        )
        fact_key = ast.Column(
            name=ast.Name(output_col_name),
            _table=ast.Table(name=ast.Name(agg_alias)),
        )
        join = build_densify_join(agg_query, spine_key, fact_key)

        # Rebuild the projection: the order dimension comes from the spine (so
        # gap dates survive), every other (measure) column is 0-filled from agg.
        new_projection: list[ast.Expression] = []
        for proj in original_select.projection:
            alias_name = proj.alias_or_name.name
            if alias_name == output_col_name:
                # Order dimension: take it from the spine domain.
                col = ast.Column(
                    name=ast.Name(spine_key_col),
                    _table=ast.Table(name=ast.Name(spine_alias)),
                )
                aliased = col.set_alias(ast.Name(output_col_name))
                aliased.set_as(True)
                new_projection.append(aliased)
            else:
                # Additive measure: COALESCE(agg.<measure>, 0).
                measure_ref = ast.Column(
                    name=ast.Name(alias_name),
                    _table=ast.Table(name=ast.Name(agg_alias)),
                )
                filled = zero_fill(measure_ref).set_alias(ast.Name(alias_name))
                filled.set_as(True)
                new_projection.append(filled)

        dense_select = ast.Select(
            projection=new_projection,
            from_=ast.From(
                relations=[
                    ast.Relation(primary=spine_subquery, extensions=[join]),
                ],
            ),
        )
        base_metrics_cte.select = dense_select


def wrap_with_output_restriction(
    result: "ast.Query",
    ctx: "BuildContext",
) -> "ast.Query":
    """
    Wrap a windowed query in an outer SELECT that re-applies the live-window
    output restriction(s) ABOVE the window.

    The inner (windowed) query computes the trailing aggregate over the
    expanded ``[R - N, R]`` scan; the wrapper then filters down to the
    originally-requested range so the lookback rows used only to seed the
    frame do not leak into the result. Filtering here (a strictly outer query
    level) is correct because the window has already been evaluated in the
    inner query.

    No-op when no live-window output restriction was registered.
    """
    if not ctx.live_window_output_restrictions:
        return result

    inner_alias = "__windowed"

    where_expr: Optional[ast.Expression] = None
    for output_col, restriction in ctx.live_window_output_restrictions:
        # Re-qualify the restriction's column to the inner subquery alias.
        qualified = ast.Between(
            expr=ast.Column(
                name=ast.Name(output_col),
                _table=ast.Table(name=ast.Name(inner_alias)),
            ),
            low=restriction.low,  # type: ignore[attr-defined]
            high=restriction.high,  # type: ignore[attr-defined]
        )
        where_expr = (
            qualified
            if where_expr is None
            else ast.BinaryOp.And(where_expr, qualified)
        )

    # Lift any ORDER BY / LIMIT off the inner windowed query and re-attach them
    # to the OUTER wrapper, so they apply ABOVE the output restriction. Left in
    # place they would order/limit the expanded ``[R - N, R]`` lookback scan
    # BEFORE the restriction strips the seed rows -- starving the requested
    # result of rows (LIMIT) and burying the final ordering inside a subquery.
    lifted_organization = result.select.organization
    lifted_limit = result.select.limit
    result.select.organization = None
    result.select.limit = None

    # The CTEs move to the outer query; the inner subquery carries only the
    # windowed SELECT, parenthesized and aliased so it renders as
    # ``(SELECT ... OVER ...) AS __windowed``.
    inner_query = ast.Query(select=result.select, ctes=[])
    inner_query.parenthesized = True
    inner_query.alias = ast.Name(inner_alias)
    inner_query.as_ = True

    wrapper_select = ast.Select(
        projection=[ast.Wildcard()],
        from_=ast.From(relations=[ast.Relation(primary=inner_query)]),
        where=where_expr,
        organization=lifted_organization,
        limit=lifted_limit,
    )
    return ast.Query(select=wrapper_select, ctes=result.ctes)
