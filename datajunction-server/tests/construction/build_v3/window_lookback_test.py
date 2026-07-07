from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3 import window_lookback as wl_mod
from datajunction_server.construction.build_v3.filters import parse_filter
from datajunction_server.construction.build_v3.measures import build_lookback_filter
from datajunction_server.construction.build_v3.window_lookback import (
    WindowLookback,
    _dimension_physical_table,
    _order_filter_bounds,
    _read_lookback_role_aware,
    _tighter_bound,
    apply_live_window_lookback,
    build_densify_join,
    build_output_restriction,
    build_scan_bounds,
    read_window_lookback,
    resolve_offset_low,
    validate_window_lookback,
    zero_fill,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


def _col():
    return ast.Column(name=ast.Name("order_date"))


def test_build_lookback_filter_between_when_both_bounds():
    low = ast.Number(20240101)
    high = ast.Number(20240131)
    result = build_lookback_filter(_col(), low, high)
    assert isinstance(result, ast.Between)
    assert result.low is low
    assert result.high is high
    assert str(result) == "order_date BETWEEN 20240101 AND 20240131"


def test_build_lookback_filter_eq_when_only_high():
    high = ast.Number(20240131)
    result = build_lookback_filter(_col(), None, high)
    assert isinstance(result, ast.BinaryOp)
    assert result.op == ast.BinaryOpKind.Eq
    assert result.right is high
    assert str(result) == "order_date = 20240131"


def test_build_lookback_filter_none_when_no_high():
    assert build_lookback_filter(_col(), None, None) is None


def _trailing_28d_expr():
    over = ast.Over(
        order_by=[
            ast.SortItem(expr=ast.Column(name=ast.Name("dateint")), asc="", nulls=""),
        ],
        window_frame=ast.Frame(
            frame_type="ROWS",
            start=ast.FrameBound(start="27", stop="PRECEDING"),
            end=ast.FrameBound(start="CURRENT", stop="ROW"),
        ),
    )
    return ast.Function(
        ast.Name("SUM"),
        args=[ast.Column(name=ast.Name("daily_visits"))],
        over=over,
    )


def test_read_window_lookback_extent_and_order_col():
    info = read_window_lookback(_trailing_28d_expr())
    assert info is not None
    assert info.extent == 27
    assert info.order_column.name.name == "dateint"
    assert info.agg_name == "SUM"


def test_read_window_lookback_none_when_no_window():
    plain = ast.Function(ast.Name("SUM"), args=[ast.Column(name=ast.Name("x"))])
    assert read_window_lookback(plain) is None


def _wl(agg="SUM"):
    return WindowLookback(
        extent=27,
        order_column=ast.Column(name=ast.Name("dateint")),
        agg_name=agg,
    )


def test_validate_accepts_additive():
    validate_window_lookback(_wl("SUM"), order_is_sequence_dim=True)  # no raise
    validate_window_lookback(_wl("COUNT"), order_is_sequence_dim=True)


def test_validate_avg_is_valid():
    """AVG is a native window function — engines support AVG() OVER (ROWS BETWEEN ...)
    directly, so validate_window_lookback must not reject it."""
    validate_window_lookback(_wl("AVG"), order_is_sequence_dim=True)  # must not raise


def test_validate_rejects_non_sequence_order_dim():
    with pytest.raises(DJInvalidInputException, match="orderable sequence dimension"):
        validate_window_lookback(_wl("SUM"), order_is_sequence_dim=False)


def test_build_densify_join_is_left_join_on_order_col():
    agg = ast.Table(ast.Name("__agg"))
    on = ast.BinaryOp.Eq(
        ast.Column(name=ast.Name("dateint")),
        ast.Column(name=ast.Name("utc_dateint")),
    )
    join = build_densify_join(agg_query=agg, on=on)
    assert isinstance(join, ast.Join)
    assert join.join_type == "LEFT"
    assert join.right is agg
    assert join.criteria.on is on
    assert isinstance(join.criteria.on, ast.BinaryOp)
    assert join.criteria.on.op == ast.BinaryOpKind.Eq
    assert str(join.criteria.on) == "dateint = utc_dateint"


def test_zero_fill_wraps_additive_measure():
    measure = ast.Column(name=ast.Name("daily_visits"))
    wrapped = zero_fill(measure)
    assert isinstance(wrapped, ast.Function)
    assert wrapped.name.name.upper() == "COALESCE"
    assert wrapped.args[0] is measure
    assert isinstance(wrapped.args[1], ast.Number) and wrapped.args[1].value == 0
    assert str(wrapped) == "COALESCE(daily_visits, 0)"


def test_build_scan_bounds_offsets_lower_only():
    col = ast.Column(name=ast.Name("dateint"))
    scan = build_scan_bounds(
        col_ref=col,
        high_expr=ast.Number(20240131),
        offset_low_expr=ast.Number(20231205),
    )
    assert isinstance(scan, ast.Between)
    assert scan.low.value == 20231205
    assert scan.high.value == 20240131
    assert str(scan) == "dateint BETWEEN 20231205 AND 20240131"


def test_build_output_restriction_reapplies_requested_predicate():
    col = ast.Column(name=ast.Name("dateint"))
    out = build_output_restriction(col, ast.Number(20240101), ast.Number(20240131))
    assert isinstance(out, ast.Between)
    assert out.low.value == 20240101
    assert out.high.value == 20240131
    assert str(out) == "dateint BETWEEN 20240101 AND 20240131"


def test_resolve_offset_low_builds_ranked_subquery():
    dim_table = ast.Table(ast.Name("v3_date"))
    expr = resolve_offset_low(
        dim_table=dim_table,
        order_col_name="dateint",
        lower_expr=ast.Number(20240101),
        extent=27,
    )
    # N positions before lower_expr needs N+1 rows ranked descending then MIN:
    # extent=27 -> LIMIT 28.
    assert str(expr) == (
        "(SELECT  MIN(__o.dateint) \n"
        " FROM (SELECT  dateint \n"
        " FROM v3_date \n"
        " WHERE  dateint <= 20240101\n"
        "ORDER BY dateint DESC\n"
        "\n"
        "LIMIT 28) __o)"
    )


def test_window_lookback_materialized_live_consistency():
    """
    Guard the headline guarantee of the frame-aware window feature: the
    MATERIALIZED (cube) path and the LIVE (/sql,/data) path apply the window
    lookback CONSISTENTLY, so the two adapters cannot drift apart.

    Consistency LEVEL asserted: SHARED-CORE INVARIANT (Step-0 option 2), the
    strongest level the build_v3 test infra honestly supports for this feature.

    Why not a higher level:
      - Data-level (option 1) is not feasible: the build_v3 suite asserts
        GENERATED SQL strings; only the /data endpoint executes against an
        engine, and no existing build_v3 test executes a window/lookback metric
        end-to-end against a real engine, so there is no executable two-path row
        comparison to extend.
      - SQL-structural full-build comparison (option 3) is not honestly
        comparable here: the cube path expresses its scan bound against the
        *current logical timestamp* (``date_id BETWEEN
        CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP() - INTERVAL '3' DAY ...) ...) AND
        CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP() ...) ...)``), while the live path
        expresses it against the *requested literal range* offset N positions
        back via a ranked scalar subquery. The two scan predicates are bound to
        different anchors by design, so their full SQL is intentionally not
        byte-comparable; comparing them would force fragile SQL extraction.

    What IS load-bearing and exact: the two paths build their lookback scan
    predicate through two adapter cores that must agree on the FILTER SHAPE for a
    given ``(order column, low bound, high bound)`` --
      - cube path:  build_temporal_filter -> ``build_lookback_filter``
      - live path:  apply_live_window_lookback -> ``build_scan_bounds``
    (Note: these are two separate functions, NOT one shared callee; that is
    exactly why drift is a risk and why this invariant must be pinned. Both
    intentionally emit the same ``col BETWEEN low AND high`` artifact.)

    The assertion is full string equality on the emitted bound AST: if anyone
    later changes one adapter's bound logic without the other -- swaps low/high,
    makes a bound exclusive, switches BETWEEN to a pair of >=/<= comparisons,
    etc. -- the rendered predicates diverge and this test fails.
    """
    order_col_name = "order_date"
    low = ast.Number(20240101)
    high = ast.Number(20240131)

    # Cube/MATERIALIZED adapter core: build_temporal_filter feeds these same
    # (col, low, high) bounds into build_lookback_filter.
    cube_bound = build_lookback_filter(
        ast.Column(name=ast.Name(order_col_name)),
        low,
        high,
    )

    # Live adapter core: apply_live_window_lookback feeds the SAME bounds into
    # build_scan_bounds (signature is col, high, offset_low).
    live_bound = build_scan_bounds(
        col_ref=ast.Column(name=ast.Name(order_col_name)),
        high_expr=high,
        offset_low_expr=low,
    )

    # Both must produce a BETWEEN over the order column...
    assert isinstance(cube_bound, ast.Between)
    assert isinstance(live_bound, ast.Between)
    # ...with the identical, full lookback range -- this is the load-bearing,
    # full-equality consistency assertion.
    assert str(cube_bound) == str(live_bound)
    assert str(cube_bound) == f"{order_col_name} BETWEEN 20240101 AND 20240131"


# ---------------------------------------------------------------------------
# read_window_lookback edge cases
# ---------------------------------------------------------------------------


def _window_func(order_expr, *, frame_start="3", frame_stop="PRECEDING"):
    """SUM(daily_visits) OVER (ORDER BY <order_expr> ROWS BETWEEN <bound> ...)."""
    over = ast.Over(
        order_by=[ast.SortItem(expr=order_expr, asc="", nulls="")],
        window_frame=ast.Frame(
            frame_type="ROWS",
            start=ast.FrameBound(start=frame_start, stop=frame_stop),
            end=ast.FrameBound(start="CURRENT", stop="ROW"),
        ),
    )
    return ast.Function(
        ast.Name("SUM"),
        args=[ast.Column(name=ast.Name("daily_visits"))],
        over=over,
    )


def test_read_window_lookback_none_when_frame_start_not_preceding():
    """A frame start that isn't `N PRECEDING` (e.g. FOLLOWING) is not a lookback."""
    fn = _window_func(ast.Column(name=ast.Name("dateint")), frame_stop="FOLLOWING")
    assert read_window_lookback(fn) is None


def test_read_window_lookback_none_when_order_expr_not_column():
    """A non-Column ORDER BY expression cannot anchor a lookback."""
    fn = _window_func(ast.Number(1))
    assert read_window_lookback(fn) is None


# ---------------------------------------------------------------------------
# _read_lookback_role_aware edge cases
# ---------------------------------------------------------------------------


def test_read_lookback_role_aware_none_when_no_over():
    fn = ast.Function(ast.Name("SUM"), args=[ast.Column(name=ast.Name("x"))])
    assert _read_lookback_role_aware(fn) is None


def test_read_lookback_role_aware_none_when_no_order_by():
    over = ast.Over(order_by=[], window_frame=None)
    fn = ast.Function(ast.Name("SUM"), args=[ast.Column(name=ast.Name("x"))], over=over)
    assert _read_lookback_role_aware(fn) is None


# ---------------------------------------------------------------------------
# _order_filter_bounds edge cases
# ---------------------------------------------------------------------------


def test_order_filter_bounds_none_when_column_absent():
    assert _order_filter_bounds(parse_filter("other_col = 5"), "date_id") is None


def test_order_filter_bounds_reads_between():
    low, high = _order_filter_bounds(
        parse_filter("date_id BETWEEN 1 AND 10"),
        "date_id",
    )
    assert low.value == 1
    assert high.value == 10


def test_order_filter_bounds_none_for_unsupported_operator():
    # `!=` constrains the column but yields no range bound to expand against.
    assert _order_filter_bounds(parse_filter("date_id != 5"), "date_id") is None


def test_order_filter_bounds_none_for_non_comparison_predicate():
    # An IN-list references the column but is neither BETWEEN nor a binary
    # comparison, so there is no low/high bound to read.
    assert _order_filter_bounds(parse_filter("date_id IN (1, 2)"), "date_id") is None


# ---------------------------------------------------------------------------
# _tighter_bound edge cases
# ---------------------------------------------------------------------------


def test_tighter_bound_upper_keeps_min():
    assert _tighter_bound(ast.Number(10), ast.Number(5), keep_max=False).value == 5
    assert _tighter_bound(ast.Number(3), ast.Number(8), keep_max=False).value == 3


def test_tighter_bound_none_when_not_both_numbers():
    # Neither a non-numeric existing bound nor candidate can be compared statically.
    assert _tighter_bound(ast.Number(1), ast.String("x"), keep_max=True) is None
    assert _tighter_bound(ast.String("x"), ast.Number(1), keep_max=True) is None


# ---------------------------------------------------------------------------
# _dimension_physical_table edge cases
# ---------------------------------------------------------------------------


def test_dimension_physical_table_skips_non_source_refs():
    """Refs that don't resolve to a SOURCE node yield no physical table."""
    ctx = SimpleNamespace(
        get_parsed_query=lambda node: parse("SELECT a FROM some.unknown.table"),
        nodes={},  # ref does not resolve -> falsy ref_node branch
    )
    dim_node = SimpleNamespace(
        type=NodeType.DIMENSION,
        current=SimpleNamespace(query="SELECT a FROM some.unknown.table"),
    )
    assert _dimension_physical_table(ctx, dim_node) is None


# ---------------------------------------------------------------------------
# apply_live_window_lookback edge cases (driven by stub BuildContexts)
# ---------------------------------------------------------------------------

_ROLE_WINDOW = (
    "SUM(v3.total_revenue) OVER "
    "(ORDER BY v3.date.date_id[order] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
)
_BARE_WINDOW = (
    "SUM(v3.total_revenue) OVER "
    "(ORDER BY v3.date.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
)


def _decomposed(combiner_sql, *, decomposable=True, name="v3.some_metric"):
    combiner = parse(f"SELECT {combiner_sql}").select.projection[0]
    return SimpleNamespace(
        combiner_ast=combiner,
        is_fully_decomposable=decomposable,
        metric_node=SimpleNamespace(name=name),
    )


def _ctx(
    decomposed_list,
    dimension_filters,
    *,
    order_node="v3.date",
    node_type=NodeType.DIMENSION,
    metric_groups=None,
):
    node = SimpleNamespace(type=node_type) if node_type is not None else None
    return SimpleNamespace(
        decomposed_metrics={d.metric_node.name: d for d in decomposed_list},
        dimension_filters=list(dimension_filters),
        nodes={order_node: node},
        metric_groups=metric_groups or [],
        upstream_pushdown_filters={},
        pushdown_consumed_filters=set(),
    )


def test_apply_raises_for_non_decomposable_window_metric():
    """A window metric whose components aren't fully additive is rejected live."""
    ctx = _ctx([_decomposed(_ROLE_WINDOW, decomposable=False)], [])
    with pytest.raises(DJInvalidInputException, match="not yet supported"):
        apply_live_window_lookback(ctx)


def test_apply_handles_window_order_without_role():
    """A window ordered by a bare (non-role-qualified) column still resolves."""
    ctx = _ctx([_decomposed(_BARE_WINDOW)], [])
    # No filter constrains the order column -> nothing to expand -> no-op.
    assert apply_live_window_lookback(ctx) is None


def test_apply_skips_filters_not_on_order_column():
    ctx = _ctx([_decomposed(_ROLE_WINDOW)], ["v3.product.category = 'electronics'"])
    assert apply_live_window_lookback(ctx) is None


def test_apply_bails_on_non_comparable_low_bound():
    # A string low bound cannot be compared/interpolated -> bail, leave filters intact.
    ctx = _ctx([_decomposed(_ROLE_WINDOW)], ["v3.date.date_id >= 'a'"])
    assert apply_live_window_lookback(ctx) is None
    assert ctx.dimension_filters == ["v3.date.date_id >= 'a'"]
    assert ctx.pushdown_consumed_filters == set()


def test_apply_bails_on_non_comparable_high_bound():
    ctx = _ctx([_decomposed(_ROLE_WINDOW)], ["v3.date.date_id <= 'z'"])
    assert apply_live_window_lookback(ctx) is None
    assert ctx.dimension_filters == ["v3.date.date_id <= 'z'"]


def test_apply_noop_when_order_dim_unreachable_from_facts(monkeypatch):
    """Bounds resolve, but no fact links to the order dimension -> no expansion."""
    monkeypatch.setattr(
        wl_mod,
        "_dimension_physical_table",
        lambda ctx, node: "cat.sch.dim_table",
    )
    ctx = _ctx(
        [_decomposed(_ROLE_WINDOW)],
        ["v3.date.date_id BETWEEN 20240101 AND 20240131"],
        metric_groups=[],  # no facts -> injected_any stays False
    )
    assert apply_live_window_lookback(ctx) is None
    # The original filter is left intact since nothing was expanded.
    assert ctx.dimension_filters == ["v3.date.date_id BETWEEN 20240101 AND 20240131"]
