"""Tests for reaggregation models."""

from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregateSpec,
    ReaggregationFunction,
    dimension_reaggregate_rules,
    dump_reaggregate_spec,
    parse_reaggregate_spec,
)


def test_dump_reaggregate_spec_from_dict():
    """
    Reaggregate specs passed as dictionaries are validated and serialized.
    """
    assert dump_reaggregate_spec(
        {
            "rules": [
                {
                    "dimension": "default.date_dim.date",
                    "fn": "last_value",
                },
            ],
        },
    ) == {
        "fn": None,
        "weight": None,
        "rules": [
            {
                "dimension": "default.date_dim.date",
                "fn": "last_value",
            },
        ],
    }


def test_dump_reaggregate_spec_from_model():
    """
    Reaggregate spec models are serialized to JSON-compatible dictionaries.
    """
    assert dump_reaggregate_spec(
        ReaggregateSpec(
            rules=[
                DimensionReaggregateRule(
                    dimension="default.date_dim.date",
                    fn=ReaggregationFunction.LAST_VALUE,
                ),
            ],
        ),
    ) == {
        "fn": None,
        "weight": None,
        "rules": [
            {
                "dimension": "default.date_dim.date",
                "fn": "last_value",
            },
        ],
    }


def test_dump_reaggregate_spec_none():
    """
    Missing reaggregate specs pass through unchanged.
    """
    assert dump_reaggregate_spec(None) is None


def test_parse_reaggregate_spec_from_model():
    """
    Parsed model inputs are returned unchanged.
    """
    spec = ReaggregateSpec(
        rules=[
            DimensionReaggregateRule(
                dimension="default.date_dim.date",
                fn=ReaggregationFunction.LAST_VALUE,
            ),
        ],
    )

    assert parse_reaggregate_spec(spec) is spec


def test_dimension_reaggregate_rules_empty_for_none():
    """
    Empty reaggregate specs have no dimension-specific rules.
    """
    assert dimension_reaggregate_rules(None) == []
