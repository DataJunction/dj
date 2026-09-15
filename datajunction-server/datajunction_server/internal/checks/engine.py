"""
Evaluating validated checks against one entity, and turning results into gates.

Everything here runs against the in-memory activation built by ``context``.
There is no database access and no compilation -- checks compile once at config
load, so the per-entity cost is the snapshot, not the size of the corpus.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from datajunction_server.internal.checks.validator import CheckGate, ValidatedCheck

Activation = dict[str, Any]


@dataclass
class CheckResult:
    """One check's verdict for one entity."""

    check: str
    gate: CheckGate
    # None when the check's `when` guard excluded this entity.
    passed: bool | None
    blocked: bool

    @property
    def skipped(self) -> bool:
        """True when the guard excluded this entity, so nothing was asserted."""
        return self.passed is None


def evaluate(
    checks: Iterable[ValidatedCheck],
    activation: Activation,
    previous_activation: Activation | None = None,
) -> list[CheckResult]:
    """Run every check against one activation and resolve its gate."""
    results = []
    for check in checks:
        if check.when is not None and not _boolean(check.when, activation):
            results.append(CheckResult(check.name, check.gate, None, blocked=False))
            continue
        passed = _boolean(check.condition, activation)
        results.append(
            CheckResult(
                check.name,
                check.gate,
                passed,
                blocked=_blocks(check, passed, previous_activation),
            ),
        )
    return results


def _blocks(
    check: ValidatedCheck,
    passed: bool,
    previous_activation: Activation | None,
) -> bool:
    if passed or check.gate == CheckGate.WARN:
        return False
    if check.gate == CheckGate.BLOCK:
        return True
    # BLOCK_ON_REGRESSION: refuse only if this check used to pass. Re-evaluating
    # the same condition against the previous state is what keeps check authors
    # out of diff logic. An entity with no prior revision, or one the guard did
    # not apply to before, has nothing to regress from.
    if previous_activation is None:
        return False
    if check.when is not None and not _boolean(check.when, previous_activation):
        return False
    return _boolean(check.condition, previous_activation)


def _boolean(expression: Any, activation: Activation) -> bool:
    """
    Evaluate to a real bool.

    The library returns errors as Values rather than raising, and bool() of an
    error value reads truthy, so the type has to be checked before the result is
    trusted. Anything other than BOOL here means the config-load validator was
    bypassed.
    """
    value = expression.eval(data=activation)
    if str(value.type()) != "BOOL":
        raise RuntimeError(f"check did not evaluate to a boolean: {value}")
    return bool(value.value())
