"""
The config-load gate: compile every check, refuse anything off the allowed
function surface, require a boolean, and evaluate each clause against fixtures.

The fixture step is the one that earns its keep -- custom_metadata binds as
``map<string, dyn>``, so a misspelled property type-checks cleanly and would
otherwise only fail on real data.
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

# Start narrow: widening is backward-compatible, narrowing is not. The
# has/all/exists/filter macros never appear, but their expansions do.
# Excluded on purpose: matches_string (unbounded re2 cost), timestamp overloads
# (irreproducible deploys), to_dyn (defeats the type checker).
ALLOWED_OVERLOADS = frozenset(
    {
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
        "size_list",
        "size_map",
        "size_string",
        "size_bytes",
        "multiply_int64",
        "add_int64",
        "add_list",  # filter macro expansion
    },
)

# A clause reading only dynamic metadata type-checks as DYN; fixture evaluation
# is what pins it to an actual boolean.
_BOOLEAN_RETURN_TYPES = ("BOOL", "DYN")
_CLAUSES = ("when", "condition")


class CheckGate(StrEnum):
    """What a failing check does to a deploy."""

    WARN = "warn"
    BLOCK = "block"
    # Refuse only if this check passed against the deployed state.
    BLOCK_ON_REGRESSION = "block_on_regression"


@dataclass
class CheckSpec:
    """One check as authored. Plain data; no manifest model is implied."""

    name: str
    condition: str
    # A raw string too, since that is what a config load hands over; an
    # unrecognised gate becomes a validation issue rather than raising.
    gate: CheckGate | str
    when: str | None = None
    description: str = ""


@dataclass
class ValidationIssue:
    """Why one clause of one check was refused."""

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
    """What is usable after a config load, and what was refused."""

    checks: list[ValidatedCheck] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.issues


def build_fixtures(declared_properties: DeclaredProperties) -> list[dict[str, Any]]:
    """
    One bare entity and one fully populated, enough to force every clause down
    both branches. Built from the declared schemas, so a property no schema
    declares fails validation rather than reading null forever.
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
