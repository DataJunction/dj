"""Tests for turning a manifest's checks and rulesets into evaluation input."""

from datajunction_server.internal.checks.manifest import (
    ResolvedRuleset,
    to_manifest_checks,
)
from datajunction_server.internal.checks.validator import CheckSpec
from datajunction_server.models.deployment import (
    DeploymentCheckSpec,
    DeploymentRulesetSpec,
    DeploymentSpec,
)

CHECKS = [
    DeploymentCheckSpec(
        name="demo.owner_present",
        description="Every entity has an owner.",
        when="node.type == 'dimension'",
        condition="size(node.owners) >= 1",
        gate="warn",
    ),
    DeploymentCheckSpec(
        name="demo.color_set",
        description="Every entity declares a color.",
        condition="node.custom_metadata.sample.color != null",
        gate="block",
    ),
    DeploymentCheckSpec(
        name="demo.shape_set",
        description="Every entity declares a shape.",
        condition="node.custom_metadata.sample.shape != null",
        gate="block_on_regression",
    ),
]

RULESETS = [
    DeploymentRulesetSpec(
        name="baseline",
        display_name="Baseline",
        checks=["demo.owner_present"],
    ),
    DeploymentRulesetSpec(
        name="colorful",
        includes=["baseline"],
        checks=["demo.color_set"],
    ),
    DeploymentRulesetSpec(
        name="strict",
        when="node.custom_metadata.sample.size != null",
        includes=["colorful", "baseline"],
        checks=["demo.shape_set"],
    ),
]


def test_checks_convert_to_check_specs():
    """The strings are carried across as authored, gate included."""
    converted = to_manifest_checks(CHECKS)
    assert converted.checks == [
        CheckSpec(
            name="demo.owner_present",
            condition="size(node.owners) >= 1",
            gate="warn",
            when="node.type == 'dimension'",
            description="Every entity has an owner.",
        ),
        CheckSpec(
            name="demo.color_set",
            condition="node.custom_metadata.sample.color != null",
            gate="block",
            when=None,
            description="Every entity declares a color.",
        ),
        CheckSpec(
            name="demo.shape_set",
            condition="node.custom_metadata.sample.shape != null",
            gate="block_on_regression",
            when=None,
            description="Every entity declares a shape.",
        ),
    ]
    assert converted.rulesets == {}


def test_omitted_blocks_convert_to_nothing():
    converted = to_manifest_checks(None)
    assert converted.checks == []
    assert converted.rulesets == {}


def test_includes_expand_transitively():
    """`strict` reaches `baseline` through `colorful` and directly: one flat set."""
    spec = DeploymentSpec(
        namespace="shared",
        nodes=[],
        checks=CHECKS,
        rulesets=RULESETS,
    )
    rulesets = to_manifest_checks(spec.checks, spec.rulesets).rulesets
    assert rulesets["baseline"] == ResolvedRuleset(
        name="baseline",
        checks=frozenset({"demo.owner_present"}),
        display_name="Baseline",
    )
    assert rulesets["colorful"].checks == frozenset(
        {"demo.owner_present", "demo.color_set"},
    )
    assert rulesets["strict"] == ResolvedRuleset(
        name="strict",
        checks=frozenset({"demo.owner_present", "demo.color_set", "demo.shape_set"}),
        when="node.custom_metadata.sample.size != null",
    )
