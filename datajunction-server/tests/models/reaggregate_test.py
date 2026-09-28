"""Tests for reaggregation models."""

import pytest
from pydantic import ValidationError

from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregateSpec,
    ReaggregationFunction,
    dimension_reaggregate_rules,
    dump_reaggregate_spec,
    parse_reaggregate_spec,
    unsupported_dimension_reaggregate_functions,
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
        "params": None,
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
        "params": None,
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


@pytest.mark.parametrize(
    "unknown_field",
    [
        {"fn": "last_value"},
        {"weight": "default.orders.quantity"},
        {
            "rules": [
                {
                    "dimension": "default.date_dim.date",
                    "fn": "last_value",
                    "weight": "default.orders.quantity",
                },
            ],
        },
    ],
)
def test_parse_reaggregate_spec_rejects_unknown_fields(unknown_field: dict):
    """Unknown fields are rejected at both reaggregate model levels."""
    with pytest.raises(ValidationError):
        parse_reaggregate_spec(unknown_field)


def test_dimension_reaggregate_rules_empty_for_none():
    """
    Empty reaggregate specs have no dimension-specific rules.
    """
    assert dimension_reaggregate_rules(None) == []


def test_unsupported_dimension_reaggregate_functions_handles_empty_and_invalid():
    """
    Unsupported dimension collapse functions are reported from parsed specs.
    """
    assert unsupported_dimension_reaggregate_functions(None) == []
    assert unsupported_dimension_reaggregate_functions(
        {
            "rules": [
                {
                    "dimension": "default.date_dim.date",
                    "fn": "sum",
                },
                {
                    "dimension": "default.date_dim.date",
                    "fn": "last_value",
                },
            ],
        },
    ) == ["sum"]


def test_empty_params_allowed():
    """
    An absent or empty `params` declaration is preserved.
    """
    assert ReaggregateSpec().params is None
    assert ReaggregateSpec(params={}).params == {}


def test_params_round_trip_through_parse():
    """
    `params` survives dict -> model -> dict without loss.
    """
    spec = parse_reaggregate_spec({"params": {"compression": 200}})
    assert spec.params == {"compression": 200}
    assert parse_reaggregate_spec(dump_reaggregate_spec(spec)).params == {
        "compression": 200,
    }
