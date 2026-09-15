"""
The config-load gate for governance checks.

Compiles every check, refuses anything outside the allowed function surface,
asserts the result is a boolean, and evaluates each clause against fixtures so a
typo cannot reach a deploy. The fixture step is the one that earns its keep:
custom_metadata binds as ``map<string, dyn>``, so a misspelled property name
type-checks cleanly and only fails once it reaches real data.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from cel_expr_python import cel

from datajunction_server.enum import StrEnum
from datajunction_server.internal.checks import ast_scan
from datajunction_server.internal.checks.context import (
    DeclaredProperties,
    build_env,
    custom_metadata,
)

# The callable surface. Start narrow: widening it is backward-compatible,
# narrowing it is not. The has/all/exists/filter macros expand into
# comprehensions whose machinery (not_strictly_false, add_list, conditional)
# shows up here too, even though the macros themselves never do.
#
# Deliberately excluded, with the reasons worth keeping in a config review:
#   matches_string  - re2 regex, unbounded evaluation cost on hostile input
#   *timestamp*     - time-dependent checks make deploy results irreproducible
#   to_dyn          - defeats the type checker
ALLOWED_OVERLOADS = frozenset(
    {
        # Boolean and comparison.
        "logical_and",
        "logical_or",
        "logical_not",
        "not_strictly_false",
        "conditional",
        "equals",
        "not_equals",
        "in_list",
        "greater_equals_int64",
        "greater_int64",
        "less_equals_int64",
        "less_int64",
        # Size, for coverage thresholds and cardinality checks.
        "size_list",
        "size_map",
        "size_string",
        "size_bytes",
        # Integer arithmetic, for ratio thresholds expressed without floats.
        "multiply_int64",
        "add_int64",
        # Emitted by filter macro expansion.
        "add_list",
    },
)

# A clause may type-check as DYN when every branch reads dynamic metadata; the
# fixture evaluation below is what pins it down to an actual boolean.
_BOOLEAN_RETURN_TYPES = ("BOOL", "DYN")
_CLAUSES = ("when", "condition")


class CheckGate(StrEnum):
    """What a failing check does to a deploy."""

    # Record the failure and carry on.
    WARN = "warn"
    # Refuse the deploy.
    BLOCK = "block"
    # Refuse only if this check passed against the deployed state.
    BLOCK_ON_REGRESSION = "block_on_regression"


@dataclass
class CheckSpec:
    """One check as authored. Plain data; no manifest model is implied."""

    name: str
    condition: str
    # Accepted as a raw string too, since this is what a config load hands over;
    # an unrecognised gate comes back as a validation issue rather than raising.
    gate: CheckGate | str
    when: str | None = None
    description: str = ""


@dataclass
class ValidationIssue:
    """Why one clause of one check was refused at load."""

    check: str
    clause: str
    problem: str


@dataclass
class ValidatedCheck:
    """A check whose clauses are compiled and cleared for evaluation."""

    name: str
    gate: CheckGate
    condition: cel.Expression
    when: cel.Expression | None = None
    description: str = ""


@dataclass
class ValidationResult:
    """The outcome of a config load: what is usable, and what was refused."""

    checks: list[ValidatedCheck] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when every check compiled and validated."""
        return not self.issues


def build_fixtures(declared_properties: DeclaredProperties) -> list[dict[str, Any]]:
    """
    Two activations, enough to force every clause down both branches.

    One entity with nothing set and one fully populated. Both are built from the
    declared schemas rather than a constant, so a property that no schema
    declares fails validation instead of silently reading null forever.
    """
    populated = {
        key: {prop: f"fixture-{prop}" for prop in properties}
        for key, properties in declared_properties.items()
    }
    return [
        {
            "node": {
                "name": "fixture.bare",
                "type": "dimension",
                "namespace": "fixture",
                "custom_metadata": custom_metadata(None, declared_properties),
                "description": "",
                "mode": "published",
                "owners": [],
                "tags": [],
                "columns": [],
                "primary_key": [],
                "dimension_links": [],
            },
            "dependencies": [],
            "previous": {
                "exists": False,
                "custom_metadata": custom_metadata(None, declared_properties),
            },
            "change": {"is_new": True, "is_removal": False},
        },
        {
            "node": {
                "name": "fixture.populated",
                "type": "transform",
                "namespace": "fixture",
                "custom_metadata": custom_metadata(populated, declared_properties),
                "description": "A fully populated fixture.",
                "mode": "published",
                "owners": [
                    {"username": "one", "email": "one@example.com"},
                    {"username": "two", "email": "two@example.com"},
                ],
                "tags": [{"name": "fixture-tag", "tag_type": "fixture-tag-type"}],
                "columns": [
                    {"name": "first", "description": "The first column."},
                    {"name": "second", "description": "The second column."},
                ],
                "primary_key": ["first"],
                "dimension_links": [
                    {
                        "role": "fixture-role",
                        "join_type": "left",
                        "default_value": "fixture-default",
                        "dimension": {
                            "name": "fixture.dimension",
                            "custom_metadata": custom_metadata(
                                populated,
                                declared_properties,
                            ),
                        },
                    },
                ],
            },
            "dependencies": [
                {
                    "name": "fixture.parent",
                    "type": "source",
                    "custom_metadata": custom_metadata(
                        populated,
                        declared_properties,
                    ),
                },
            ],
            "previous": {
                "exists": True,
                "custom_metadata": custom_metadata(populated, declared_properties),
            },
            "change": {"is_new": False, "is_removal": True},
        },
    ]


def validate(
    checks: Iterable[CheckSpec],
    declared_properties: DeclaredProperties,
    env: cel.Env | None = None,
) -> ValidationResult:
    """Compile and vet every check; a check with any issue is left out."""
    env = env or build_env()
    fixtures = build_fixtures(declared_properties)
    result = ValidationResult()

    for spec in checks:
        compiled: dict[str, cel.Expression] = {}
        failed = False
        for clause in _CLAUSES:
            source = getattr(spec, clause)
            if source is None:
                continue
            issue, expression = _compile_clause(
                env,
                spec.name,
                clause,
                source,
                fixtures,
            )
            if issue:
                result.issues.append(issue)
                failed = True
            else:
                compiled[clause] = expression
        if failed:
            continue
        try:
            gate = CheckGate(spec.gate)
        except ValueError:
            result.issues.append(
                ValidationIssue(spec.name, "gate", f"unknown gate {spec.gate!r}"),
            )
            continue
        result.checks.append(
            ValidatedCheck(
                name=spec.name,
                gate=gate,
                condition=compiled["condition"],
                when=compiled.get("when"),
                description=spec.description,
            ),
        )
    return result


def _compile_clause(
    env: cel.Env,
    name: str,
    clause: str,
    source: str,
    fixtures: Sequence[dict[str, Any]],
) -> tuple[ValidationIssue | None, cel.Expression | None]:
    try:
        expression = env.compile(source)
    except Exception as exc:  # the library raises RuntimeError with the CEL error
        return ValidationIssue(name, clause, f"does not compile: {exc}"), None

    # return_type() is a method here, not a property.
    return_type = str(expression.return_type())
    if return_type not in _BOOLEAN_RETURN_TYPES:
        return (
            ValidationIssue(name, clause, f"must yield a boolean, got {return_type}"),
            None,
        )

    denied = ast_scan.called_overloads(expression.serialize()) - ALLOWED_OVERLOADS
    if denied:
        return (
            ValidationIssue(
                name,
                clause,
                f"calls functions outside the allowed surface: {sorted(denied)}",
            ),
            None,
        )

    for fixture in fixtures:
        value = expression.eval(data=fixture)
        if str(value.type()) != "BOOL":
            return (
                ValidationIssue(
                    name,
                    clause,
                    f"evaluated to {value.type()} on fixture "
                    f"{fixture['node']['name']!r}: {value}",
                ),
                None,
            )
    return None, expression
