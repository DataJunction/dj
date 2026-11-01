"""Tests for the steps that run before a metrics query is built."""

from unittest.mock import MagicMock

import pytest

from datajunction_server.construction.build_v3 import prepare
from datajunction_server.construction.build_v3.filter_requirements import (
    CheckedFilters,
)
from datajunction_server.construction.build_v3.prepare import (
    PreparedMetricsQuery,
    prepare_metrics_query,
)
from datajunction_server.errors import DJWarning, ErrorCode
from datajunction_server.models.dialect import Dialect

WARNING = DJWarning(
    code=ErrorCode.DEFAULT_FILTER_APPLIED,
    message="Applied default filter d.x = 1 (from metric m)",
)


@pytest.fixture
def calls(monkeypatch):
    """Replace the lookups with recorders."""
    seen: dict = {"check": [], "validate": [], "resolve": []}
    execution = MagicMock()
    execution.dialect = Dialect.TRINO

    async def fake_check(session, metrics, filters, inherit=False):
        seen["check"].append((metrics, filters))
        return CheckedFilters([*(filters or []), "d.x = 1"], [WARNING])

    async def fake_validate(session, cube, dimensions, filters, metrics):
        seen["validate"].append((cube, dimensions, filters, metrics))

    async def fake_resolve(**kwargs):
        seen["resolve"].append(kwargs)
        return execution

    monkeypatch.setattr(prepare, "check_filter_requirements", fake_check)
    monkeypatch.setattr(prepare, "validate_pinned_cube_covers_filters", fake_validate)
    monkeypatch.setattr(prepare, "resolve_dialect_and_engine_for_metrics", fake_resolve)
    seen["execution"] = execution
    return seen


@pytest.mark.asyncio
async def test_checks_filters_and_returns_them_with_warnings(calls):
    prepared = await prepare_metrics_query(
        MagicMock(),
        ["m"],
        ["d.y"],
        ["d.y = 2"],
    )
    assert prepared == PreparedMetricsQuery(
        CheckedFilters(["d.y = 2", "d.x = 1"], [WARNING]),
        None,
    )
    assert calls["check"] == [(["m"], ["d.y = 2"])]
    assert calls["validate"] == []
    assert calls["resolve"] == []


@pytest.mark.asyncio
async def test_skips_the_check_when_not_enforced(calls):
    prepared = await prepare_metrics_query(
        MagicMock(),
        ["m"],
        [],
        None,
        dialect=Dialect.SPARK,
        enforce_filter_requirements=False,
    )
    assert prepared == PreparedMetricsQuery(CheckedFilters([]), Dialect.SPARK)
    assert calls["check"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dialect", "use_materialized", "validated"),
    [
        (None, True, True),
        (Dialect.DRUID, True, True),
        (Dialect.SPARK, True, False),
        (None, False, False),
    ],
)
async def test_validates_a_pinned_cube_only_when_it_backs_the_build(
    calls,
    dialect,
    use_materialized,
    validated,
):
    cube = MagicMock()
    await prepare_metrics_query(
        MagicMock(),
        ["m"],
        ["d.y"],
        ["d.y = 2"],
        matched_cube=cube,
        use_materialized=use_materialized,
        dialect=dialect,
    )
    assert calls["validate"] == (
        [(cube, ["d.y"], ["d.y = 2", "d.x = 1"], ["m"])] if validated else []
    )


@pytest.mark.asyncio
async def test_does_not_validate_without_a_cube_or_metrics(calls):
    await prepare_metrics_query(MagicMock(), ["m"], [], None)
    await prepare_metrics_query(MagicMock(), [], [], None, matched_cube=MagicMock())
    assert calls["validate"] == []


@pytest.mark.asyncio
async def test_resolves_the_engine_on_the_checked_filters(calls):
    session = MagicMock()
    cube = MagicMock()
    prepared = await prepare_metrics_query(
        session,
        ["m"],
        ["d.y"],
        ["d.y = 2"],
        matched_cube=cube,
        use_materialized=False,
        dialect=Dialect.SPARK,
        engine_name="e",
        engine_version="1",
        resolve_execution=True,
    )
    assert prepared == PreparedMetricsQuery(
        CheckedFilters(["d.y = 2", "d.x = 1"], [WARNING]),
        Dialect.TRINO,
        calls["execution"],
    )
    assert calls["resolve"] == [
        {
            "session": session,
            "metrics": ["m"],
            "dimensions": ["d.y"],
            "use_materialized": False,
            "engine_name": "e",
            "engine_version": "1",
            "dialect_override": Dialect.SPARK,
            "matched_cube": cube,
            "filters": ["d.y = 2", "d.x = 1"],
        },
    ]


@pytest.mark.asyncio
async def test_resolver_gets_no_cube_for_a_query_without_metrics(calls):
    await prepare_metrics_query(
        MagicMock(),
        [],
        ["d.y"],
        None,
        matched_cube=MagicMock(),
        resolve_execution=True,
    )
    assert calls["resolve"][0]["matched_cube"] is None
