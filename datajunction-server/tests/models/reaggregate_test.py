"""Tests for reaggregation models."""

import pytest
from pydantic import ValidationError

from datajunction_server.models import reaggregate as reaggregate_module
from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregateSpec,
    ReaggregationFunction,
    dimension_reaggregate_rules,
    dump_reaggregate_spec,
    is_parameterized_reaggregate_function,
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
        "fn": None,
        "weight": None,
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
        "fn": None,
        "weight": None,
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


def test_params_rejected_for_unparameterized_function():
    """
    Functions without tuneable parameters reject `params`.
    """
    with pytest.raises(ValidationError) as excinfo:
        ReaggregateSpec(fn=ReaggregationFunction.SUM, params={"compression": 200})
    assert "`sum` does not accept parameters" in str(excinfo.value)
    assert "compression" in str(excinfo.value)


def test_params_rejected_when_no_function_given():
    """
    `params` without an `fn` is rejected.
    """
    with pytest.raises(ValidationError) as excinfo:
        ReaggregateSpec(params={"k": 256})
    assert "no reaggregation function does not accept parameters" in str(excinfo.value)


def test_empty_params_allowed():
    """
    An absent or empty `params` is fine on any function.
    """
    assert ReaggregateSpec(fn=ReaggregationFunction.SUM).params is None
    assert ReaggregateSpec(fn=ReaggregationFunction.SUM, params={}).params == {}


def test_params_accepted_for_parameterized_function(monkeypatch):
    """
    Registering a function as parameterized enables `params` assignment.
    """
    monkeypatch.setattr(
        reaggregate_module,
        "PARAMETERIZED_REAGGREGATE_FUNCTIONS",
        frozenset({ReaggregationFunction.AVG}),
    )
    spec = ReaggregateSpec(fn=ReaggregationFunction.AVG, params={"compression": 200})
    assert spec.params == {"compression": 200}
    assert dump_reaggregate_spec(spec)["params"] == {"compression": 200}


def test_only_sketch_families_are_parameterized():
    """
    Only a sketch family takes tuning parameters.

    The rollup functions are fully specified by their name -- there is nothing
    to tune about a SUM. A sketch is not: a t-digest still needs a compression,
    which is why `params` is accepted for it and rejected everywhere else.
    """
    parameterized = {
        fn for fn in ReaggregationFunction if is_parameterized_reaggregate_function(fn)
    }

    assert parameterized == {ReaggregationFunction.TDIGEST}


def test_params_round_trip_through_parse():
    """
    `params` survives dict -> model -> dict without loss.
    """
    monkeypatched = frozenset({ReaggregationFunction.AVG})
    original = reaggregate_module.PARAMETERIZED_REAGGREGATE_FUNCTIONS
    reaggregate_module.PARAMETERIZED_REAGGREGATE_FUNCTIONS = monkeypatched
    try:
        spec = parse_reaggregate_spec({"fn": "avg", "params": {"compression": 200}})
        assert spec.params == {"compression": 200}
        assert parse_reaggregate_spec(dump_reaggregate_spec(spec)).params == {
            "compression": 200,
        }
    finally:
        reaggregate_module.PARAMETERIZED_REAGGREGATE_FUNCTIONS = original
