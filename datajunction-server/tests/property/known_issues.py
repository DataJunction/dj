"""
Bugs the property tests have found and that are not yet fixed.

Generated tests exclude cases affected by known bugs, so they can keep
searching. Each bug also has a small repro marked `xfail(strict=True)`: when
the bug is fixed the repro passes, pytest reports it as XPASS and fails, and
that is the cue to delete the entry here along with its exclusions.
"""

from dataclasses import dataclass

import pytest

from tests.property.comparison import PropertyMismatch


@dataclass(frozen=True)
class KnownIssue:
    summary: str
    issue: str | None = None  # link once filed

    @property
    def reason(self) -> str:
        return self.summary + (f" ({self.issue})" if self.issue else "")

    def xfail(self):
        # Only the expected oracle mismatch is an XFAIL. Setup/API assertions
        # and unrelated exceptions must still fail the test.
        return pytest.mark.xfail(
            reason=self.reason,
            strict=True,
            raises=PropertyMismatch,
        )

    def skip(self):
        return pytest.mark.skip(reason=f"known issue: {self.reason}")


PRINTER_DROPS_PARENTHESES = KnownIssue(
    "SQL assembled from AST nodes built in code prints without the parentheses "
    "its nesting needs: sample variance/stddev/correlation combiners, combined "
    "filters, derived metrics over compound metrics, and `-(-x)` as `--x`",
)
COVARIANCE_COUNTS_HALF_NULL_ROWS = KnownIssue(
    "COVAR_POP/COVAR_SAMP/CORR decompositions count rows where only one of the "
    "two arguments is NULL",
)
COUNT_COMPONENT_NAME_COLLISION = KnownIssue(
    "COUNT(x) from AVG/COUNT/VAR and from COVAR/CORR share a component name but "
    "record the aggregation as `COUNT` vs `COUNT(x)`, so frozen measures conflict",
)
EMPTY_PREAGG_COUNT_IS_NULL = KnownIssue(
    "a COUNT served from a pre-aggregation with no matching rows is NULL rather than 0",
)
FILTER_PUSHED_PAST_LEFT_JOIN = KnownIssue(
    "a dimension filter is also applied inside the LEFT-joined dimension, so a "
    "filter that accepts NULL (e.g. `label IS NULL`) matches facts whose "
    "dimension row it filtered out",
)
PREAGG_INNER_JOIN_DROPS_ORPHANS = KnownIssue(
    "a pre-aggregation planned with an INNER-linked dimension drops facts with "
    "no matching dimension row, so requests that don't use that dimension get "
    "a different answer when served from it",
)
