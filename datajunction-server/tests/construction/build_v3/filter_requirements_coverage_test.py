"""build_metrics_sql checks filter requirements unless told they were checked."""

import pytest

from datajunction_server.construction.build_v3 import (
    CheckedFilters,
    build_metrics_sql,
    check_filter_requirements,
)
from datajunction_server.errors import DJInvalidInputException

from .filter_requirements_endpoints_test import DEFAULT_ENTRY, _make_metric


@pytest.mark.asyncio
async def test_plain_filters_are_checked(client_with_build_v3, session):
    await _make_metric(
        client_with_build_v3,
        "v3.cov_required",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    with pytest.raises(DJInvalidInputException) as exc:
        await build_metrics_sql(
            session=session,
            metrics=["v3.cov_required"],
            dimensions=["v3.location.country"],
            filters=[],
        )
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of v3.date.date_id[order] "
        "(required by metric v3.cov_required)",
    ]


@pytest.mark.asyncio
async def test_plain_filters_get_defaults_and_warnings(client_with_build_v3, session):
    await _make_metric(
        client_with_build_v3,
        "v3.cov_default",
        required_filters=[DEFAULT_ENTRY],
    )
    result = await build_metrics_sql(
        session=session,
        metrics=["v3.cov_default"],
        dimensions=["v3.location.country[from]"],
    )
    assert [warning.message for warning in result.warnings] == [
        "Applied default filter v3.date.date_id[order] = 20250101 "
        "(from metric v3.cov_default)",
    ]


@pytest.mark.asyncio
async def test_checked_filters_are_not_checked_again(client_with_build_v3, session):
    await _make_metric(
        client_with_build_v3,
        "v3.cov_checked",
        required_filters=[DEFAULT_ENTRY],
    )
    checked = await check_filter_requirements(session, ["v3.cov_checked"], [])
    result = await build_metrics_sql(
        session=session,
        metrics=["v3.cov_checked"],
        dimensions=["v3.location.country[from]"],
        filters=checked,
    )
    # The default was applied once, by the first check.
    assert [warning.message for warning in result.warnings] == [
        "Applied default filter v3.date.date_id[order] = 20250101 "
        "(from metric v3.cov_checked)",
    ]
    assert result.sql.count("20250101") == 1


@pytest.mark.asyncio
async def test_unchecked_filters_skip_the_check(client_with_build_v3, session):
    await _make_metric(
        client_with_build_v3,
        "v3.cov_skipped",
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    result = await build_metrics_sql(
        session=session,
        metrics=["v3.cov_skipped"],
        dimensions=["v3.location.country[from]"],
        filters=CheckedFilters([]),
    )
    assert result.warnings == []
