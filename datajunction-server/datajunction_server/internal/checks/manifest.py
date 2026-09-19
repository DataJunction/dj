"""
Turn a manifest's `checks` and `rulesets` blocks into what the evaluation layer
takes. A check is one rule; a ruleset is a named bundle of them, so a manifest
can talk about a tier rather than a list.

    checks:
      - name: demo.owner_present
        description: Every entity has an owner.
        condition: "size(node.owners) >= 1"
        gate: warn

      - name: demo.primary_key_set
        description: Dimensions declare a primary key.
        when: "node.type == 'dimension'"     # only applies to dimensions
        condition: "size(node.primary_key) >= 1"
        gate: block

A check carries its own verdict and its own consequence. A ruleset has no
conditions of its own -- it is composition:

    rulesets:
      - name: baseline
        display_name: Baseline
        checks: [demo.owner_present, demo.primary_key_set]

      - name: strict
        when: "node.custom_metadata.sample.size != null"  # the tier is scoped
        includes: [baseline]                              # all of baseline, plus
        checks: [demo.column_descriptions]

The two `when` guards stack: a ruleset's says which entities the tier applies
to, a check's says which entities that one rule applies to.

Nothing here compiles an expression -- the strings are carried across as
authored and vetted by `load_checks`.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from datajunction_server.internal.checks.validator import CheckSpec
from datajunction_server.models.deployment import (
    DeploymentCheckSpec,
    DeploymentRulesetSpec,
)


@dataclass
class ResolvedRuleset:
    """A ruleset with its `includes` folded in."""

    name: str
    checks: frozenset[str]
    display_name: str | None = None
    when: str | None = None


@dataclass
class ManifestChecks:
    """The checks a manifest declares, and the rulesets that select them."""

    checks: list[CheckSpec] = field(default_factory=list)
    rulesets: dict[str, ResolvedRuleset] = field(default_factory=dict)


def to_manifest_checks(
    checks: Iterable[DeploymentCheckSpec] | None,
    rulesets: Sequence[DeploymentRulesetSpec] | None = None,
) -> ManifestChecks:
    """Convert declared checks and rulesets. Assumes the spec already validated."""
    return ManifestChecks(
        checks=[to_check_spec(check) for check in checks or []],
        rulesets=resolve_rulesets(rulesets or []),
    )


def to_check_spec(check: DeploymentCheckSpec) -> CheckSpec:
    return CheckSpec(
        name=check.name,
        condition=check.condition,
        gate=check.gate,
        when=check.when,
        description=check.description,
    )


def resolve_rulesets(
    rulesets: Sequence[DeploymentRulesetSpec],
) -> dict[str, ResolvedRuleset]:
    """Flatten each ruleset to the check names it selects, `includes` expanded."""
    by_name = {ruleset.name: ruleset for ruleset in rulesets}
    flattened: dict[str, frozenset[str]] = {}

    def flatten(name: str) -> frozenset[str]:
        if name not in flattened:
            ruleset = by_name[name]
            names = set(ruleset.checks)
            for included in ruleset.includes:
                names |= flatten(included)
            flattened[name] = frozenset(names)
        return flattened[name]

    return {
        ruleset.name: ResolvedRuleset(
            name=ruleset.name,
            checks=flatten(ruleset.name),
            display_name=ruleset.display_name,
            when=ruleset.when,
        )
        for ruleset in rulesets
    }
