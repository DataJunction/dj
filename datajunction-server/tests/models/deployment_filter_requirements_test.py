"""MetricSpec behavior for filter requirements."""

from datajunction_server.models.deployment import (
    ChangeTier,
    MetricSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.engine import local_node_fingerprint

BASE = {
    "name": "m",
    "namespace": "pricing",
    "query": "SELECT SUM(x) FROM ${prefix}items",
}


def spec(**fields) -> MetricSpec:
    return MetricSpec(**BASE, **fields)


def test_rendered_resolves_prefix_in_dimension_and_default():
    metric = spec(
        required_filters=[
            {
                "dimension": "${prefix}version.is_current",
                "default": "${prefix}version.is_current = true",
            },
        ],
    )
    rendered = metric.rendered_required_filters
    assert rendered is not None
    assert rendered[0].dimension == ("pricing.version.is_current",)
    assert rendered[0].default == "pricing.version.is_current = true"


def test_filter_requirement_dimensions_strip_roles():
    metric = spec(
        required_filters=[{"dimension": "${prefix}date.day[order]"}],
        recommended_filters=[{"dimension": ["${prefix}geo.country"]}],
    )
    assert metric.filter_requirement_dimensions == [
        "pricing.date.day",
        "pricing.geo.country",
    ]


def test_equal_ignores_order_and_string_vs_list():
    first = spec(required_filters=[{"dimension": "a.b"}, {"dimension": ["c.d", "e.f"]}])
    second = spec(
        required_filters=[{"dimension": ["f.e"]}, {"dimension": ["e.f", "c.d"]}],
    )
    assert first != second
    same = spec(
        required_filters=[{"dimension": ["c.d", "e.f"]}, {"dimension": ["a.b"]}],
    )
    assert first == same


def test_diff_reports_change_and_tier_is_major():
    old = spec(required_filters=[{"dimension": "a.b"}])
    new = spec(required_filters=[{"dimension": "a.c"}])
    assert new.diff(old) == ["required_filters"]
    assert MetricSpec.field_change_tier("required_filters") == ChangeTier.MAJOR
    assert MetricSpec.field_change_tier("recommended_filters") == ChangeTier.MAJOR


def test_diff_ignores_reorder():
    old = spec(required_filters=[{"dimension": "a.b"}, {"dimension": "c.d"}])
    new = spec(required_filters=[{"dimension": "c.d"}, {"dimension": "a.b"}])
    assert new.diff(old) == []


def test_diff_ignores_reorder_inside_an_entry():
    old = spec(required_filters=[{"dimension": ["a.b", "c.d"]}])
    new = spec(required_filters=[{"dimension": ["c.d", "a.b"]}])
    assert new.diff(old) == []


def test_diff_reports_recommended_change():
    old = spec(recommended_filters=[{"dimension": "a.b"}])
    new = spec(recommended_filters=[{"dimension": "a.c"}])
    assert new.diff(old) == ["recommended_filters"]


def test_diff_against_another_node_type_skips_filter_comparison():
    other = TransformSpec(name="m", namespace="pricing", query="SELECT 1")
    assert spec(required_filters=[{"dimension": "a.b"}]).diff(other) == ["node_type"]


def test_fingerprint_ignores_reorder_and_tracks_default():
    first = spec(
        required_filters=[
            {"dimension": ["a.b", "c.d"], "default": "a.b = 1"},
            {"dimension": "e.f"},
        ],
    )
    reordered = spec(
        required_filters=[
            {"dimension": "e.f"},
            {"dimension": ["c.d", "a.b"], "default": "a.b = 1"},
        ],
    )
    changed = spec(
        required_filters=[
            {"dimension": ["a.b", "c.d"], "default": "a.b = 2"},
            {"dimension": "e.f"},
        ],
    )
    assert local_node_fingerprint(first) == local_node_fingerprint(reordered)
    assert local_node_fingerprint(first) != local_node_fingerprint(changed)
    assert local_node_fingerprint(first) != local_node_fingerprint(spec())
