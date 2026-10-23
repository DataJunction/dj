"""Tests for metric filter requirement models."""

import pytest
from pydantic import ValidationError

from datajunction_server.models.filter_requirements import (
    FilterRequirement,
    attribute_node_name,
    dump_filter_requirements,
    filter_requirements_identity,
    parse_filter_requirements,
)


def test_string_dimension_becomes_tuple():
    entry = FilterRequirement.model_validate(
        {"dimension": "pricing.version.is_current"},
    )
    assert entry.dimension == ("pricing.version.is_current",)
    assert entry.default is None


def test_list_dimension_and_default():
    entry = FilterRequirement.model_validate(
        {
            "dimension": ["pricing.version.version_id", "pricing.version.is_current"],
            "default": "pricing.version.is_current = true",
        },
    )
    assert entry.dimension == (
        "pricing.version.version_id",
        "pricing.version.is_current",
    )
    assert entry.default == "pricing.version.is_current = true"


@pytest.mark.parametrize(
    "payload",
    [
        {"dimension": []},
        {"dimension": "a.b", "unknown": 1},
        {"default": "a.b = 1"},
    ],
)
def test_invalid_entries_rejected(payload):
    with pytest.raises(ValidationError):
        FilterRequirement.model_validate(payload)


def test_dump_round_trip_omits_empty_default():
    entries = parse_filter_requirements(
        [{"dimension": "a.b"}, {"dimension": ["a.c", "a.d"], "default": "a.c = 1"}],
    )
    assert dump_filter_requirements(entries) == [
        {"dimension": ["a.b"]},
        {"dimension": ["a.c", "a.d"], "default": "a.c = 1"},
    ]
    assert parse_filter_requirements(dump_filter_requirements(entries)) == entries


def test_none_stays_none():
    assert parse_filter_requirements(None) is None
    assert dump_filter_requirements(None) is None


def test_identity_ignores_order():
    first = [{"dimension": ["a.b", "a.c"]}, {"dimension": "x.y", "default": "x.y = 1"}]
    second = [{"dimension": "x.y", "default": "x.y = 1"}, {"dimension": ["a.c", "a.b"]}]
    assert filter_requirements_identity(first) == filter_requirements_identity(second)
    assert filter_requirements_identity(first) != filter_requirements_identity(
        [{"dimension": "a.b"}],
    )


def test_attribute_node_name():
    assert attribute_node_name("v3.date.date_id") == "v3.date"
    assert attribute_node_name("v3.date.date_id[order]") == "v3.date"
