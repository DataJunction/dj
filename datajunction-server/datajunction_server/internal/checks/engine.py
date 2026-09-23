"""Evaluate validated checks against one entity and resolve their gates."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from datajunction_server.internal.checks.context import Bindings
from datajunction_server.internal.checks.validator import CheckGate, CompiledCheck


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
        return self.passed is None


def evaluate(
    checks: Iterable[CompiledCheck],
    bindings: Bindings,
    previous_bindings: Bindings | None = None,
) -> list[CheckResult]:
    """Run every check against one bindings and resolve its gate."""
    results = []
    for check in checks:
        if check.when is not None and not _boolean(check.when, bindings):
            results.append(CheckResult(check.name, check.gate, None, blocked=False))
            continue
        passed = _boolean(check.condition, bindings)
        results.append(
            CheckResult(
                check.name,
                check.gate,
                passed,
                blocked=_blocks(check, passed, previous_bindings),
            ),
        )
    return results


def _blocks(
    check: CompiledCheck,
    passed: bool,
    previous_bindings: Bindings | None,
) -> bool:
    if passed or check.gate == CheckGate.WARN:
        return False
    if check.gate == CheckGate.BLOCK:
        return True
    # BLOCK_ON_REGRESSION: re-evaluate against the previous state, so authors
    # never write diff logic. Nothing prior means nothing to regress from.
    if previous_bindings is None:
        return False
    if check.when is not None and not _boolean(check.when, previous_bindings):
        return False
    return _boolean(check.condition, previous_bindings)


def _boolean(expression: Any, bindings: Bindings) -> bool:
    """Evaluate to a real bool."""
    value = expression.eval(data=bindings)
    if str(value.type()) != "BOOL":
        raise RuntimeError(f"check did not evaluate to a boolean: {value}")
    return bool(value.value())
