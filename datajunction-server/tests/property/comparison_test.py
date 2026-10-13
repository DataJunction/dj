"""The oracle must not conflate distinct SQL result values."""

import math

import pytest

from tests.property.comparison import same_value


@pytest.mark.parametrize(
    ("actual", "expected", "matches"),
    [
        (None, None, True),
        (None, math.nan, False),
        (None, math.inf, False),
        (math.nan, math.nan, True),
        (math.nan, math.inf, False),
        (math.inf, math.inf, True),
        (math.inf, -math.inf, False),
        (1.0, 1.0 + 1e-7, True),
        (1.0, 1.1, False),
    ],
)
def test_same_value(actual, expected, matches):
    assert same_value(actual, expected) is matches
