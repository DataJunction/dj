"""Presentation metadata and warning-only filters do not change metric results."""

import asyncio
import copy

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    PartitionSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)
from tests.internal.deployment.fingerprints_v2_configured_test import (
    specs as _configured_specs,
)


def _specs():
    specs = _configured_specs()
    del specs["ns.region"]
    return specs


def _clear_query_caches(nodes):
    copied = copy.deepcopy(nodes)
    for spec in copied.values():
        if hasattr(spec, "_query_ast"):
            spec._query_ast = None
    return copied


def _evaluate(nodes):
    snapshot = SemanticGraphSnapshot(_clear_query_caches(nodes))
    return asyncio.run(compute_configured_v2_fingerprints(snapshot))


def _assert_unchanged(before, after, *names):
    for name in names:
        assert before.nodes[name].full.digest is not None
        assert after.nodes[name].full.digest is not None
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "unchanged"
        )


@pytest.mark.parametrize("default", [None, ""])
def test_recommended_filter_without_effective_default_is_only_a_warning(default):
    before_specs = _specs()
    before = _evaluate(before_specs)

    after_specs = _specs()
    after_specs["ns.total"].recommended_filters = [
        FilterRequirement(dimension=["ns.region.name"], default=default),
    ]
    after_specs["ns.region"] = after_specs["ns.country"].model_copy(
        update={"name": "ns.region"},
    )
    after = _evaluate(after_specs)

    assert after.nodes["ns.total"].precision != "unknown"
    _assert_unchanged(before, after, "ns.total", "ns.by_country")


def test_missing_dimension_mentioned_only_by_warning_is_ignored():
    before = _evaluate(_specs())
    after_specs = _specs()
    after_specs["ns.total"].recommended_filters = [
        FilterRequirement(dimension=["ns.region.name"]),
    ]
    after = _evaluate(after_specs)

    assert after.nodes["ns.total"].precision != "unknown"
    _assert_unchanged(before, after, "ns.total", "ns.by_country")


def test_recommended_filter_default_remains_an_effective_dependency():
    before_specs = _specs()
    before_specs["ns.total"].recommended_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name = 'US'",
        ),
    ]
    before = _evaluate(before_specs)

    after_specs = copy.deepcopy(before_specs)
    after_specs["ns.total"].recommended_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default="ns.country.name = 'CA'",
        ),
    ]
    after = _evaluate(after_specs)

    assert before.nodes["ns.total"].full.digest is not None
    assert after.nodes["ns.total"].full.digest is not None
    assert (
        compare_components(before.nodes["ns.total"].full, after.nodes["ns.total"].full)
        == "changed"
    )
    assert (
        compare_components(
            before.nodes["ns.by_country"].full,
            after.nodes["ns.by_country"].full,
        )
        == "changed"
    )


def test_presentation_metadata_is_retained_but_not_hashed_on_used_paths():
    before_specs = _specs()
    after_specs = copy.deepcopy(before_specs)

    for name in ("ns.fact", "ns.lookup", "ns.country", "ns.total", "ns.by_country"):
        after_specs[name] = after_specs[name].model_copy(
            update={
                "description": f"Updated description for {name}",
                "display_name": f"Label for {name}",
                "owners": ["data-stewards"],
                "tags": ["domain:metadata-test"],
                "custom_metadata": {"display": {"summary": "presentation only"}},
            },
            deep=True,
        )

    for name in ("ns.fact", "ns.lookup", "ns.country"):
        spec = after_specs[name]
        spec.columns = [
            column.model_copy(
                update={
                    "description": f"Updated {column.name} description",
                    "display_name": f"Label {column.name}",
                    "attributes": [*column.attributes, "hidden"],
                },
            )
            for column in spec.columns or []
        ]

    before = _evaluate(before_specs)
    after = _evaluate(after_specs)

    _assert_unchanged(
        before,
        after,
        "ns.fact",
        "ns.lookup",
        "ns.country",
        "ns.total",
        "ns.by_country",
    )
    assert after_specs["ns.country"].description == "Updated description for ns.country"
    assert (
        after_specs["ns.country"].columns[1].description == "Updated name description"
    )
    assert after_specs["ns.country"].columns[1].display_name == "Label name"
    assert "hidden" in after_specs["ns.country"].columns[1].attributes
    assert after_specs["ns.country"].custom_metadata == {
        "display": {"summary": "presentation only"},
    }


def test_spark_hint_is_excluded_but_cube_partition_is_hashed():
    before_specs = _specs()
    before = _evaluate(before_specs)
    after_specs = copy.deepcopy(before_specs)
    link = after_specs["ns.fact"].dimension_links[0]
    after_specs["ns.fact"].dimension_links = [
        link.model_copy(update={"spark_hints": "broadcast"}),
    ]
    hinted = _evaluate(after_specs)
    _assert_unchanged(before, hinted, "ns.total", "ns.by_country")

    after_specs["ns.by_country"].columns = [
        ColumnSpec(
            name="ns.country.name",
            partition=PartitionSpec(type="temporal", granularity="day"),
        ),
    ]
    partitioned = _evaluate(after_specs)
    assert (
        compare_components(
            hinted.nodes["ns.by_country"].full,
            partitioned.nodes["ns.by_country"].full,
        )
        == "changed"
    )


@pytest.mark.parametrize("default", ["ns.country.name >", " "])
def test_malformed_recommended_default_remains_unknown(default):
    specs = _specs()
    specs["ns.total"].recommended_filters = [
        FilterRequirement(
            dimension=["ns.country.name"],
            default=default,
        ),
    ]
    result = _evaluate(specs)

    assert result.nodes["ns.total"].precision == "unknown"
    assert result.nodes["ns.by_country"].precision == "unknown"
