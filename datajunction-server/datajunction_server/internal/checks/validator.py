"""
Validate checks at config load: compile and evaluate each clause against
fixtures. Evaluating catches a misspelled custom_metadata property, since
metadata values are dynamically typed.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from cel_expr_python import cel

from datajunction_server.enum import StrEnum
from datajunction_server.internal.checks import allowlist
from datajunction_server.internal.checks.context import Bindings, build_env

_BOOLEAN_RETURN_TYPES = ("BOOL", "DYN")


class CheckGate(StrEnum):
    """What a failing check does to a deploy."""

    WARN = "warn"
    BLOCK = "block"
    # Refuse only if this check passed against the deployed state.
    BLOCK_ON_REGRESSION = "block_on_regression"


@dataclass
class CheckSpec:
    """A check specification."""

    name: str
    condition: str
    # A raw string too, since that is what a config load hands over; an
    # unrecognized gate is reported as malformed rather than raising.
    gate: CheckGate | str
    when: str | None = None
    description: str = ""


@dataclass
class MalformedCheck:
    """Why a check is malformed, found at config load."""

    check: str
    clause: str
    problem: str


@dataclass
class CompiledCheck:
    """A compiled check, ready to evaluate."""

    name: str
    gate: CheckGate
    condition: cel.Expression
    when: cel.Expression | None = None
    description: str = ""


@dataclass
class LoadedChecks:
    """The checks that loaded, and what was wrong with the rest."""

    checks: list[CompiledCheck] = field(default_factory=list)
    malformed: list[MalformedCheck] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.malformed


def load_checks(
    checks: Iterable[CheckSpec],
    fixtures: Sequence[Bindings],
    env: cel.Env | None = None,
) -> LoadedChecks:
    """Compile and vet every check. Malformed checks are split out and not run."""
    env = env or build_env()
    result = LoadedChecks()

    for spec in checks:
        when = None
        if spec.when is not None:
            issue, when = _compile_clause(env, spec.name, "when", spec.when, fixtures)
            if issue:
                result.malformed.append(issue)
                continue
        issue, condition = _compile_clause(
            env,
            spec.name,
            "condition",
            spec.condition,
            _applicable(when, fixtures),
        )
        if issue:
            result.malformed.append(issue)
            continue
        try:
            gate = CheckGate(spec.gate)
        except ValueError:
            result.malformed.append(
                MalformedCheck(spec.name, "gate", f"unknown gate {spec.gate!r}"),
            )
            continue
        result.checks.append(
            CompiledCheck(
                name=spec.name,
                gate=gate,
                condition=condition,
                when=when,
                description=spec.description,
            ),
        )
    return result


def _applicable(
    when: cel.Expression | None,
    fixtures: Sequence[Bindings],
) -> Sequence[Bindings]:
    """
    The fixtures a guarded condition is judged against.

    A condition only has to hold where its `when` does, so judging it on a
    fixture the guard excludes rejects checks that would never have run. When
    no fixture satisfies the guard the condition is judged against all of them,
    since evaluating it somewhere is what catches a misspelled property.
    """
    if when is None:
        return fixtures
    admitted = [
        fixture
        for fixture in fixtures
        if str((value := when.eval(data=fixture)).type()) == "BOOL" and value.value()
    ]
    return admitted or fixtures


def _compile_clause(
    env: cel.Env,
    name: str,
    clause: str,
    source: str,
    fixtures: Sequence[Bindings],
) -> tuple[MalformedCheck | None, cel.Expression | None]:
    try:
        expression = env.compile(source)
    except Exception as exc:  # the library raises RuntimeError with the CEL error
        return MalformedCheck(name, clause, f"does not compile: {exc}"), None

    # return_type() is a method here, not a property.
    return_type = str(expression.return_type())
    if return_type not in _BOOLEAN_RETURN_TYPES:
        return (
            MalformedCheck(name, clause, f"must yield a boolean, got {return_type}"),
            None,
        )

    denied = allowlist.denied_overloads(expression.serialize())
    if denied:
        return (
            MalformedCheck(
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
                MalformedCheck(
                    name,
                    clause,
                    f"evaluated to {value.type()} on fixture "
                    f"{fixture['node']['name']!r}: {value}",
                ),
                None,
            )
    return None, expression
