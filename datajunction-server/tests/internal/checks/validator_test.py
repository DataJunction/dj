"""Tests for the config-load gate: compile, allowlist, type, fixture evaluation."""

import pytest

from datajunction_server.internal.checks.context import build_env
from datajunction_server.internal.checks.validator import (
    CheckGate,
    CheckSpec,
    build_fixtures,
    validate,
)
from tests.internal.checks.conftest import DECLARED_PROPERTIES

# An invented corpus, exercising every binding without borrowing any real
# governance vocabulary.
CHECKS = [
    CheckSpec(
        name="demo.colour_set",
        description="Every entity declares a colour.",
        condition="node.custom_metadata.sample.colour != null",
        gate=CheckGate.WARN,
    ),
    CheckSpec(
        name="demo.owner_present",
        description="At least one owner, with a resolvable email.",
        condition=(
            "size(node.owners) >= 1"
            " && node.owners.all(o, o.email != null && o.email != '')"
        ),
        gate=CheckGate.WARN,
    ),
    CheckSpec(
        name="demo.primary_key_set",
        description="Dimensions declare a primary key.",
        when="node.type == 'dimension'",
        condition="size(node.primary_key) >= 1",
        gate=CheckGate.BLOCK,
    ),
    CheckSpec(
        name="demo.flavour_tagged",
        description="A flavour tag is set.",
        condition="node.tags.exists(t, t.tag_type == 'flavour')",
        gate=CheckGate.WARN,
    ),
    CheckSpec(
        name="demo.column_descriptions",
        description="At least 70% of columns are described.",
        condition=(
            "size(node.columns.filter(c, c.description != null"
            " && c.description != '')) * 10 >= size(node.columns) * 7"
        ),
        gate=CheckGate.WARN,
    ),
    CheckSpec(
        name="demo.no_red_dependencies",
        description="Not depending on anything red.",
        condition="dependencies.all(d, d.custom_metadata.sample.colour != 'red')",
        gate=CheckGate.BLOCK_ON_REGRESSION,
    ),
    CheckSpec(
        name="demo.shape_before_removal",
        description="A shape is recorded before an entity is removed.",
        when="change.is_removal",
        condition="previous.custom_metadata.sample.shape != null",
        gate=CheckGate.BLOCK,
    ),
]


@pytest.fixture(scope="module")
def validated():
    result = validate(CHECKS, DECLARED_PROPERTIES)
    assert result.ok, [vars(issue) for issue in result.issues]
    return result


def test_the_corpus_compiles_and_type_checks(validated):
    assert [check.name for check in validated.checks] == [spec.name for spec in CHECKS]
    assert validated.checks[2].when is not None
    assert validated.checks[0].description == "Every entity declares a colour."


def _one(**overrides):
    fields = {"name": "demo.probe", "condition": "node.name != ''", "gate": "warn"}
    return validate([CheckSpec(**{**fields, **overrides})], DECLARED_PROPERTIES)


@pytest.mark.parametrize(
    ("condition", "reason"),
    [
        ("node.description.matches('^[A-Z]')", "re2 regex is off the surface"),
        ("node.name.startsWith('default.')", "string prefix matching is not allowed"),
    ],
)
def test_off_surface_functions_are_refused(condition, reason):
    result = _one(condition=condition)
    assert not result.ok
    assert result.checks == []
    assert "outside the allowed surface" in result.issues[0].problem, reason


def test_undeclared_binding_fails_to_compile():
    result = _one(condition="warehouse.owner != null")
    assert not result.ok
    assert "does not compile" in result.issues[0].problem


def test_non_boolean_condition_is_refused():
    result = _one(condition="size(node.owners)")
    assert not result.ok
    assert "must yield a boolean, got INT" in result.issues[0].problem


def test_a_bad_guard_is_reported_against_its_own_clause():
    result = _one(when="size(node.owners)")
    assert [(i.clause, "INT" in i.problem) for i in result.issues] == [("when", True)]


def test_misspelled_metadata_property_is_rejected_at_load():
    """
    The gap the type checker cannot close: custom_metadata is map<string, dyn>,
    so this compiles cleanly. Fixture evaluation is what turns it into a
    config-load failure rather than a silent error on every real entity.
    """
    misspelled = "node.custom_metadata.sample.colur != null"
    assert str(build_env().compile(misspelled).return_type()) in ("BOOL", "DYN")

    result = _one(condition=misspelled)
    assert not result.ok
    assert "evaluated to ERROR" in result.issues[0].problem


def test_a_property_no_schema_declares_is_rejected():
    # The fixtures come from the declared schemas, so a property that was
    # renamed out from under a check fails here instead of reading null forever.
    result = validate(
        [CheckSpec("demo.probe", "node.custom_metadata.sample.tint != null", "warn")],
        {"sample": ("colour",)},
    )
    assert not result.ok
    assert "evaluated to ERROR" in result.issues[0].problem


def test_unknown_gate_is_refused():
    result = _one(gate="nag")
    assert not result.ok
    assert result.issues[0].problem == "unknown gate 'nag'"


def test_prefill_makes_an_unset_property_false_not_an_error():
    bare, _ = build_fixtures(DECLARED_PROPERTIES)
    compiled = build_env().compile("node.custom_metadata.sample.colour != null")
    value = compiled.eval(data=bare)
    assert str(value.type()) == "BOOL"
    assert value.value() is False


def test_unschematized_paths_are_reachable_with_stepwise_guards():
    """
    No schema is needed to read metadata. Optional chaining and orValue are
    compile errors in this version, so a guard is a stepwise has() chain; && is
    short-circuiting, which makes that safe.
    """
    env = build_env()
    bare, _ = build_fixtures(DECLARED_PROPERTIES)
    present = dict(bare)
    present["node"] = dict(
        bare["node"],
        custom_metadata={
            **bare["node"]["custom_metadata"],
            "freeform": {"nested": {"leaf": "found"}},
        },
    )

    guarded = env.compile(
        "has(node.custom_metadata.freeform)"
        " && has(node.custom_metadata.freeform.nested)"
        " && node.custom_metadata.freeform.nested.leaf == 'found'",
    )
    assert guarded.eval(data=present).value() is True
    # The same guard short-circuits to false rather than erroring when absent.
    assert guarded.eval(data=bare).value() is False

    # Unguarded, an absent key is an error value, not false.
    unguarded = env.compile("node.custom_metadata.absent.leaf == 1")
    assert str(unguarded.eval(data=bare).type()) == "ERROR"


@pytest.mark.parametrize("unsupported", ["node.custom_metadata.?sample", "node.x"])
def test_guards_cannot_be_written_with_optional_chaining(unsupported):
    # Recorded so an upgrade that adds `.?` support shows up as a test failure.
    with pytest.raises(Exception):
        build_env().compile(f"{unsupported}.orValue(1) == 1")


def test_has_is_true_for_a_present_but_null_property():
    # Why "has a value" must be written as `!= null`: the prefilled properties
    # are present, so has() cannot distinguish them from authored ones.
    bare, _ = build_fixtures(DECLARED_PROPERTIES)
    compiled = build_env().compile("has(node.custom_metadata.sample.colour)")
    assert compiled.eval(data=bare).value() is True


def test_fixtures_are_built_from_the_declared_properties():
    bare, populated = build_fixtures({"sample": ("colour",), "other": ("weight",)})
    assert bare["node"]["custom_metadata"] == {
        "sample": {"colour": None},
        "other": {"weight": None},
    }
    assert populated["node"]["custom_metadata"]["other"]["weight"] == "fixture-weight"
    assert populated["previous"]["exists"] is True
    assert populated["change"] == {"is_new": False, "is_removal": True}
