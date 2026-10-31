"""Tests for metric filter requirement enforcement."""

from unittest.mock import MagicMock

import pytest

from datajunction_server.construction.build_v3.filter_requirements import (
    RequirementEntry,
    apply_metric_filter_requirements,
    apply_node_filter_requirements,
    apply_requirements,
    load_effective_filters,
    split_effective_entries,
)
from datajunction_server.models.filter_requirements import EffectiveFilterRequirement
from datajunction_server.database.queryrequest import QueryBuildType
from datajunction_server.errors import DJInvalidInputException, DJWarning, ErrorCode
from datajunction_server.internal.caching.cachelib_cache import CachelibCache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)


def entry(attributes, default=None, required=True, metrics=("m",)):
    return RequirementEntry(
        attributes=tuple(attributes),
        default=default,
        required=required,
        metrics=frozenset(metrics),
    )


def test_no_entries_returns_filters_unchanged():
    assert apply_requirements([], ["a.b = 1"]) == (["a.b = 1"], [])


def test_missing_required_without_default_raises_naming_attribute_and_metric():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements([entry(["pricing.version.version_id"])], [])
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of pricing.version.version_id "
        "(required by metric m)",
    ]
    assert [error.code for error in exc.value.errors] == [
        ErrorCode.REQUIRED_FILTER_MISSING,
    ]


def test_all_missing_required_filters_reported_together():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements([entry(["a.x"]), entry(["b.y"], metrics=("n",))], [])
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of a.x (required by metric m)",
        "Missing required filter: one of b.y (required by metric n)",
    ]


def test_missing_recommended_warns_and_keeps_filters():
    filters, warnings = apply_requirements(
        [entry(["a.x"], required=False)],
        ["b.y = 1"],
    )
    assert filters == ["b.y = 1"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.RECOMMENDED_FILTER_MISSING,
            "Missing recommended filter: one of a.x (recommended by metric m)",
        ),
    ]


def test_default_is_appended_and_reported():
    filters, warnings = apply_requirements(
        [entry(["v.is_current"], default="v.is_current = true")],
        ["d.day = 1"],
    )
    assert filters == ["d.day = 1", "v.is_current = True"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.DEFAULT_FILTER_APPLIED,
            "Applied default filter v.is_current = True (from metric m)",
        ),
    ]


def test_caller_filter_replaces_default():
    filters, warnings = apply_requirements(
        [entry(["v.version_id", "v.is_current"], default="v.is_current = true")],
        ["v.version_id = 7"],
    )
    assert filters == ["v.version_id = 7"]
    assert warnings == []


def test_filter_on_other_attribute_of_same_node_does_not_replace_default():
    filters, _ = apply_requirements(
        [entry(["v.is_current"], default="v.is_current = true")],
        ["v.version_id = 7"],
    )
    assert filters == ["v.version_id = 7", "v.is_current = True"]


def test_role_rules():
    plain = entry(["d.day"], default="d.day = 1")
    with_role = entry(["d.day[order]"], default="d.day[order] = 1")
    assert apply_requirements([plain], ["d.day[ship] = 5"])[0] == ["d.day[ship] = 5"]
    assert apply_requirements([with_role], ["d.day[order] = 5"])[0] == [
        "d.day[order] = 5",
    ]
    assert apply_requirements([with_role], ["d.day[ship] = 5"])[0] == [
        "d.day[ship] = 5",
        "d.day[order] = 1",
    ]


def test_function_wrapped_filter_counts():
    filters, _ = apply_requirements(
        [entry(["d.day"])],
        ["DATE(d.day) = DATE('2025-01-01')"],
    )
    assert filters == ["DATE(d.day) = DATE('2025-01-01')"]


def test_same_entry_from_two_metrics_merges():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements(
            [entry(["a.x"], metrics=("m",)), entry(["a.x"], metrics=("n",))],
            [],
        )
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of a.x (required by metric m, n)",
    ]


def test_required_beats_recommended_for_same_entry():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements(
            [entry(["a.x"], required=False), entry(["a.x"], required=True)],
            [],
        )
    assert [error.code for error in exc.value.errors] == [
        ErrorCode.REQUIRED_FILTER_MISSING,
    ]
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of a.x (required by metric m)",
    ]


def test_identical_defaults_apply_once():
    filters, _ = apply_requirements(
        [
            entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
            entry(["v.is_current"], default="v.is_current  =  true", metrics=("n",)),
        ],
        [],
    )
    assert filters == ["v.is_current = True"]


def test_conflicting_defaults_on_same_attribute_raise():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements(
            [
                entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
                entry(["v.is_current"], default="v.is_current = false", metrics=("n",)),
            ],
            [],
        )
    assert [error.code for error in exc.value.errors] == [
        ErrorCode.CONFLICTING_DEFAULT_FILTERS,
    ]
    assert [error.message for error in exc.value.errors] == [
        "Conflicting default filters on v.is_current: "
        "v.is_current = True (metric m) and v.is_current = False (metric n)",
    ]


def test_defaults_on_same_column_under_different_roles_do_not_conflict():
    filters, warnings = apply_requirements(
        [
            entry(["cal.day"], default="cal.day = 1", metrics=("m",)),
            entry(["cal.day[ship]"], default="cal.day[ship] = 2", metrics=("n",)),
        ],
        [],
    )
    assert filters == ["cal.day = 1", "cal.day[ship] = 2"]
    assert [warning.message for warning in warnings] == [
        "Applied default filter cal.day = 1 (from metric m)",
        "Applied default filter cal.day[ship] = 2 (from metric n)",
    ]


def test_default_from_other_metric_does_not_satisfy_required_entry():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements(
            [
                entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
                entry(["v.is_current"], metrics=("n",)),
            ],
            [],
        )
    assert [(error.code, error.message) for error in exc.value.errors] == [
        (
            ErrorCode.REQUIRED_FILTER_MISSING,
            "Missing required filter: one of v.is_current (required by metric n)",
        ),
    ]


def test_default_from_other_metric_does_not_satisfy_recommended_entry():
    filters, warnings = apply_requirements(
        [
            entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
            entry(["v.is_current"], required=False, metrics=("n",)),
        ],
        [],
    )
    assert filters == ["v.is_current = True"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.RECOMMENDED_FILTER_MISSING,
            "Missing recommended filter: one of v.is_current (recommended by metric n)",
        ),
        (
            ErrorCode.DEFAULT_FILTER_APPLIED,
            "Applied default filter v.is_current = True (from metric m)",
        ),
    ]


def test_same_default_on_different_attribute_lists_applies_once():
    filters, warnings = apply_requirements(
        [
            entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
            entry(
                ["v.is_current", "v.version_id"],
                default="v.is_current = true",
                metrics=("n",),
            ),
        ],
        [],
    )
    assert filters == ["v.is_current = True"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.DEFAULT_FILTER_APPLIED,
            "Applied default filter v.is_current = True (from metric m, n)",
        ),
    ]


def test_default_differing_in_spacing_and_case_is_not_a_conflict():
    filters, warnings = apply_requirements(
        [
            entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
            entry(
                ["v.is_current", "v.version_id"],
                default="v.is_current  =  TRUE",
                metrics=("n",),
            ),
        ],
        [],
    )
    assert filters == ["v.is_current = True"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.DEFAULT_FILTER_APPLIED,
            "Applied default filter v.is_current = True (from metric m, n)",
        ),
    ]


def test_different_attribute_lists_with_different_defaults_conflict():
    with pytest.raises(DJInvalidInputException) as exc:
        apply_requirements(
            [
                entry(["v.is_current"], default="v.is_current = true", metrics=("m",)),
                entry(
                    ["v.is_current", "v.version_id"],
                    default="v.is_current = false",
                    metrics=("n",),
                ),
            ],
            [],
        )
    assert [(error.code, error.message) for error in exc.value.errors] == [
        (
            ErrorCode.CONFLICTING_DEFAULT_FILTERS,
            "Conflicting default filters on v.is_current: "
            "v.is_current = True (metric m) and v.is_current = False (metric n)",
        ),
    ]


async def _create_metric(client, name, query, **fields):
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": "d",
            "query": query,
            "mode": "published",
            **fields,
        },
    )
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_loader_applies_default_for_requested_metric(
    client_with_build_v3,
    session,
):
    query = (await client_with_build_v3.get("/nodes/v3.total_revenue/")).json()["query"]
    await _create_metric(
        client_with_build_v3,
        "v3.rf_default",
        query,
        required_filters=[
            {"dimension": "v3.date.date_id", "default": "v3.date.date_id = 20250101"},
        ],
    )
    filters, warnings = await apply_metric_filter_requirements(
        session,
        ["v3.rf_default"],
        [],
    )
    assert filters == ["v3.date.date_id = 20250101"]
    assert [(w.code, w.message) for w in warnings] == [
        (
            ErrorCode.DEFAULT_FILTER_APPLIED,
            "Applied default filter v3.date.date_id = 20250101 "
            "(from metric v3.rf_default)",
        ),
    ]


@pytest.mark.asyncio
async def test_derived_metric_inherits_base_requirements(client_with_build_v3, session):
    query = (await client_with_build_v3.get("/nodes/v3.total_revenue/")).json()["query"]
    await _create_metric(
        client_with_build_v3,
        "v3.rf_base",
        query,
        required_filters=[{"dimension": "v3.date.date_id"}],
    )
    await _create_metric(
        client_with_build_v3,
        "v3.rf_derived",
        "SELECT v3.rf_base / 2",
    )
    with pytest.raises(DJInvalidInputException) as exc:
        await apply_metric_filter_requirements(session, ["v3.rf_derived"], [])
    assert [error.message for error in exc.value.errors] == [
        "Missing required filter: one of v3.date.date_id (required by metric v3.rf_base)",
    ]


@pytest.mark.asyncio
async def test_no_metrics_or_no_entries_changes_nothing(client_with_build_v3, session):
    assert await apply_metric_filter_requirements(session, [], ["a.b = 1"]) == (
        ["a.b = 1"],
        [],
    )
    assert await apply_metric_filter_requirements(
        session,
        ["v3.total_revenue"],
        None,
    ) == ([], [])


def test_second_pass_adds_no_defaults():
    entries = [
        entry(["a.x"], default="a.x = 1"),
        entry(["b.y"], required=False),
    ]
    effective, _ = apply_requirements(entries, ["c.z = 2"])
    assert effective == ["c.z = 2", "a.x = 1"]
    assert apply_requirements(entries, effective) == (
        ["c.z = 2", "a.x = 1"],
        [
            DJWarning(
                code=ErrorCode.RECOMMENDED_FILTER_MISSING,
                message="Missing recommended filter: one of b.y "
                "(recommended by metric m)",
            ),
        ],
    )


@pytest.mark.asyncio
async def test_node_check_skips_other_node_types(client_with_build_v3, session):
    filters = ["v3.date.date_id = 1"]
    assert await apply_node_filter_requirements(
        session,
        "v3.order_details",
        filters,
    ) == (filters, [])
    assert await apply_node_filter_requirements(session, "v3.missing", filters) == (
        filters,
        [],
    )


@pytest.mark.asyncio
async def test_node_cache_key_unchanged_without_entries(client_with_build_v3, session):
    manager = QueryCacheManager(CachelibCache(), QueryBuildType.NODE)
    params = QueryRequestParams(
        nodes=["v3.total_revenue"],
        dimensions=["v3.location.country"],
        filters=["v3.date.date_id = 20240101"],
    )
    request = MagicMock(headers={})
    checked, warnings = await manager._apply_filter_requirements(
        request,
        params,
        session,
    )
    assert (checked, warnings) == (params, [])
    assert await manager.build_cache_key(
        request,
        checked,
        session,
    ) == await manager.build_cache_key(request, params, session)


def effective(dimension, default=None, declared_by=("m",)):
    return EffectiveFilterRequirement(
        dimension=tuple(dimension),
        default=default,
        declared_by=tuple(declared_by),
    )


def test_split_effective_entries_empty():
    assert split_effective_entries([]) == ([], [])


def test_split_effective_entries_required_beats_recommended():
    assert split_effective_entries(
        [
            entry(["a.x"], required=False, metrics=("m",)),
            entry(["a.x"], required=True, metrics=("n",)),
            entry(["a.y"], required=False, metrics=("n",)),
        ],
    ) == (
        [effective(["a.x"], declared_by=("m", "n"))],
        [effective(["a.y"], declared_by=("n",))],
    )


def test_split_effective_entries_lists_conflicting_defaults():
    assert split_effective_entries(
        [
            entry(["a.x"], default="a.x = 1", metrics=("n",)),
            entry(["a.x"], default="a.x = 2", metrics=("m",)),
        ],
    ) == (
        [
            effective(["a.x"], default="a.x = 1", declared_by=("n",)),
            effective(["a.x"], default="a.x = 2", declared_by=("m",)),
        ],
        [],
    )


@pytest.mark.asyncio
async def test_effective_filters_follow_upstream_metrics(client_with_build_v3, session):
    query = (await client_with_build_v3.get("/nodes/v3.total_revenue/")).json()["query"]
    await _create_metric(
        client_with_build_v3,
        "v3.rf_a",
        query,
        required_filters=[{"dimension": "v3.date.date_id"}],
        recommended_filters=[{"dimension": "v3.location.country"}],
    )
    await _create_metric(
        client_with_build_v3,
        "v3.rf_b",
        query,
        recommended_filters=[{"dimension": "v3.date.date_id"}],
    )
    await _create_metric(client_with_build_v3, "v3.rf_mid", "SELECT v3.rf_a / 2")
    await _create_metric(
        client_with_build_v3,
        "v3.rf_top",
        "SELECT v3.rf_mid / v3.rf_b",
        recommended_filters=[{"dimension": "v3.location.city"}],
    )
    assert await load_effective_filters(session, []) == {}
    assert await load_effective_filters(
        session,
        ["v3.rf_a", "v3.rf_mid", "v3.rf_top", "v3.total_revenue", "v3.order_details"],
    ) == {
        "v3.rf_a": (
            [effective(["v3.date.date_id"], declared_by=("v3.rf_a",))],
            [effective(["v3.location.country"], declared_by=("v3.rf_a",))],
        ),
        "v3.rf_mid": (
            [effective(["v3.date.date_id"], declared_by=("v3.rf_a",))],
            [effective(["v3.location.country"], declared_by=("v3.rf_a",))],
        ),
        "v3.rf_top": (
            [effective(["v3.date.date_id"], declared_by=("v3.rf_a", "v3.rf_b"))],
            [
                effective(["v3.location.country"], declared_by=("v3.rf_a",)),
                effective(["v3.location.city"], declared_by=("v3.rf_top",)),
            ],
        ),
        "v3.total_revenue": ([], []),
        "v3.order_details": ([], []),
    }
