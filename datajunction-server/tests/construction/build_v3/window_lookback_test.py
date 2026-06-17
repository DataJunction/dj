from datajunction_server.sql.parsing import ast
from datajunction_server.construction.build_v3.measures import build_lookback_filter


def _col():
    return ast.Column(name=ast.Name("order_date"))


def test_build_lookback_filter_between_when_both_bounds():
    low = ast.Number(20240101)
    high = ast.Number(20240131)
    result = build_lookback_filter(_col(), low, high)
    assert isinstance(result, ast.Between)
    assert result.low is low
    assert result.high is high


def test_build_lookback_filter_eq_when_only_high():
    high = ast.Number(20240131)
    result = build_lookback_filter(_col(), None, high)
    assert isinstance(result, ast.BinaryOp)
    assert result.op == ast.BinaryOpKind.Eq
    assert result.right is high


def test_build_lookback_filter_none_when_no_high():
    assert build_lookback_filter(_col(), None, None) is None


from datajunction_server.construction.build_v3.window_lookback import read_window_lookback


def _trailing_28d_expr():
    over = ast.Over(
        order_by=[ast.SortItem(expr=ast.Column(name=ast.Name("dateint")), asc="", nulls="")],
        window_frame=ast.Frame(
            frame_type="ROWS",
            start=ast.FrameBound(start="27", stop="PRECEDING"),
            end=ast.FrameBound(start="CURRENT", stop="ROW"),
        ),
    )
    return ast.Function(ast.Name("SUM"), args=[ast.Column(name=ast.Name("daily_visits"))], over=over)


def test_read_window_lookback_extent_and_order_col():
    info = read_window_lookback(_trailing_28d_expr())
    assert info is not None
    assert info.extent == 27
    assert info.order_column.name.name == "dateint"
    assert info.agg_name == "SUM"


def test_read_window_lookback_none_when_no_window():
    plain = ast.Function(ast.Name("SUM"), args=[ast.Column(name=ast.Name("x"))])
    assert read_window_lookback(plain) is None


import pytest
from datajunction_server.construction.build_v3.window_lookback import (
    WindowLookback, validate_window_lookback,
)
from datajunction_server.errors import DJInvalidInputException


def _wl(agg="SUM"):
    return WindowLookback(extent=27, order_column=ast.Column(name=ast.Name("dateint")), agg_name=agg)


def test_validate_accepts_additive():
    validate_window_lookback(_wl("SUM"), order_is_sequence_dim=True)  # no raise
    validate_window_lookback(_wl("COUNT"), order_is_sequence_dim=True)


def test_validate_rejects_non_additive():
    with pytest.raises(DJInvalidInputException, match="additive"):
        validate_window_lookback(_wl("AVG"), order_is_sequence_dim=True)


def test_validate_rejects_non_sequence_order_dim():
    with pytest.raises(DJInvalidInputException, match="orderable sequence dimension"):
        validate_window_lookback(_wl("SUM"), order_is_sequence_dim=False)


from datajunction_server.construction.build_v3.window_lookback import build_densify_join, zero_fill


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


def test_zero_fill_wraps_additive_measure():
    measure = ast.Column(name=ast.Name("daily_visits"))
    wrapped = zero_fill(measure)
    assert isinstance(wrapped, ast.Function)
    assert wrapped.name.name.upper() == "COALESCE"
    assert wrapped.args[0] is measure
    assert isinstance(wrapped.args[1], ast.Number) and wrapped.args[1].value == 0


from datajunction_server.construction.build_v3.window_lookback import (
    build_scan_bounds, build_output_restriction,
)


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


def test_build_output_restriction_reapplies_requested_predicate():
    col = ast.Column(name=ast.Name("dateint"))
    out = build_output_restriction(col, ast.Number(20240101), ast.Number(20240131))
    assert isinstance(out, ast.Between)
    assert out.low.value == 20240101
    assert out.high.value == 20240131


from datajunction_server.construction.build_v3.window_lookback import resolve_offset_low


def test_resolve_offset_low_builds_ranked_subquery():
    dim_table = ast.Table(ast.Name("v3_date"))
    expr = resolve_offset_low(
        dim_table=dim_table,
        order_col_name="dateint",
        lower_expr=ast.Number(20240101),
        extent=27,
    )
    rendered = str(expr).upper()
    assert "DATEINT" in rendered
    # The offset (limit/rank) appears: N positions before `lower` needs N+1 rows
    # ranked descending, then MIN. extent=27 -> LIMIT 28.
    assert "28" in rendered


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
