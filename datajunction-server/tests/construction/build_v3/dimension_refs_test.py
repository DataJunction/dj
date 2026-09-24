"""Tests for low-level dimension reference parsing."""

import pytest

from datajunction_server.construction.build_v3.dimension_refs import (
    split_dimension_ref,
)


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        ("v3.date.date_id", ("v3.date.date_id", None)),
        ("v3.date.date_id[order]", ("v3.date.date_id", "order")),
        ("date_id[order]", ("date_id", "order")),
        (
            "v3.date.date_id[customer->registration]",
            ("v3.date.date_id", "customer->registration"),
        ),
    ],
)
def test_split_dimension_ref(ref, expected):
    """Qualified and bare refs share the same role parsing."""
    assert split_dimension_ref(ref) == expected
