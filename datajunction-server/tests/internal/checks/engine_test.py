"""Tests for evaluation and the three gates."""

import pytest

from datajunction_server.internal.checks.engine import evaluate
from datajunction_server.internal.checks.validator import (
    CheckGate,
    CheckSpec,
    load_checks,
)
from tests.internal.checks.conftest import DECLARED_PROPERTIES, build_fixtures

CHECKS = [
    CheckSpec("demo.color_set", "node.custom_metadata.sample.color != null", "warn"),
    CheckSpec("demo.primary_key_set", "size(node.primary_key) >= 1", "block"),
    CheckSpec(
        "demo.size_set",
        "node.custom_metadata.sample.size != null",
        "block_on_regression",
    ),
    CheckSpec(
        "demo.dimension_shape_set",
        "node.custom_metadata.sample.shape != null",
        "block_on_regression",
        when="node.node_type == 'dimension'",
    ),
]


@pytest.fixture(scope="module")
def checks():
    result = load_checks(CHECKS, build_fixtures(DECLARED_PROPERTIES))
    assert result.ok, [vars(issue) for issue in result.malformed]
    return result.checks


@pytest.fixture
def fixtures():
    return build_fixtures(DECLARED_PROPERTIES)


def _by_name(results):
    return {result.check: result for result in results}


def test_warn_records_a_failure_without_blocking(checks, fixtures):
    results = _by_name(evaluate(checks, fixtures[0]))
    assert results["demo.color_set"].passed is False
    assert results["demo.color_set"].blocked is False
    assert results["demo.color_set"].gate == CheckGate.WARN
    assert results["demo.color_set"].skipped is False


def test_block_refuses_the_deploy(checks, fixtures):
    results = _by_name(evaluate(checks, fixtures[0]))
    assert results["demo.primary_key_set"].passed is False
    assert results["demo.primary_key_set"].blocked is True


def test_a_passing_check_never_blocks(checks, fixtures):
    results = _by_name(evaluate(checks, fixtures[1]))
    assert all(
        result.passed is True and result.blocked is False
        for result in results.values()
        if not result.skipped
    )


def test_a_guard_skips_an_inapplicable_entity(checks, fixtures):
    # The populated fixture is a transform, so the dimension-only check is
    # skipped and asserts nothing either way.
    skipped = _by_name(evaluate(checks, fixtures[1]))["demo.dimension_shape_set"]
    assert skipped.skipped is True
    assert skipped.passed is None
    assert skipped.blocked is False


def test_block_on_regression_does_not_block_without_previous_state(checks, fixtures):
    failing = _by_name(evaluate(checks, fixtures[0]))["demo.size_set"]
    assert failing.passed is False
    assert failing.blocked is False


def test_block_on_regression_blocks_when_the_check_used_to_pass(checks, fixtures):
    empty, populated = fixtures
    regressed = _by_name(
        evaluate(checks, empty, previous_activation=populated),
    )["demo.size_set"]
    assert regressed.blocked is True


def test_block_on_regression_tolerates_a_pre_existing_failure(checks, fixtures):
    empty, _ = fixtures
    # Failing before and failing now is not a regression.
    already = _by_name(evaluate(checks, empty, previous_activation=empty))[
        "demo.size_set"
    ]
    assert already.passed is False
    assert already.blocked is False


def test_block_on_regression_skips_a_guard_that_did_not_apply_before(checks, fixtures):
    empty, populated = fixtures
    # The entity was a transform before and is a dimension now, so the guarded
    # check has no prior verdict to regress from.
    result = _by_name(
        evaluate(checks, empty, previous_activation=populated),
    )["demo.dimension_shape_set"]
    assert result.passed is False
    assert result.blocked is False


def test_a_non_boolean_result_is_a_hard_error(checks, fixtures):
    # Only reachable if the config-load validator was bypassed: the library
    # returns errors as values, and bool() of an error value reads truthy.
    unvalidated = load_checks(
        [CheckSpec("demo.probe", "node.custom_metadata.sample.color != null", "warn")],
        build_fixtures(DECLARED_PROPERTIES),
    ).checks
    broken = dict(fixtures[0])
    broken["node"] = dict(broken["node"], custom_metadata={})
    with pytest.raises(RuntimeError, match="did not evaluate to a boolean"):
        evaluate(unvalidated, broken)
