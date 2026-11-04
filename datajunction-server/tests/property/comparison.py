"""Value comparison and failure type for property-test correctness oracles."""

import math


class PropertyMismatch(AssertionError):
    """The result of a DJ operation differs from the independent answer."""


def same_value(actual, expected) -> bool:
    """Compare numbers approximately, but keep SQL NULL and non-finite values distinct."""
    if actual is None or expected is None:
        return actual is None and expected is None
    if isinstance(actual, float) and math.isnan(actual):
        return isinstance(expected, float) and math.isnan(expected)
    if isinstance(expected, float) and math.isnan(expected):
        return False
    return math.isclose(actual, expected, rel_tol=1e-6, abs_tol=1e-6)
