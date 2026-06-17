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
