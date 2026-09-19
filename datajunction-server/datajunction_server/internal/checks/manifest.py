"""
Turn a manifest's `checks` and `rulesets` blocks into what the evaluation layer
takes. Nothing here compiles an expression: the strings are carried across as
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
