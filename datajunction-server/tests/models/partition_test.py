"""
Tests for ``datajunction_server.models.partition``.
"""

import pytest

from datajunction_server.models.partition import (
    Granularity,
    PartitionGrain,
    strftime_format,
)


@pytest.mark.parametrize(
    "format_, expected",
    [
        ("yyyyMMdd", "%Y%m%d"),
        ("yyyy-MM-dd", "%Y-%m-%d"),
        ("yyyyMMddHHmmss", "%Y%m%d%H%M%S"),
        ("", None),
        (None, None),
        ("payment_id", None),
    ],
)
def test_strftime_format(format_, expected) -> None:
    """
    Test translating partition formats into strftime patterns.
    """
    assert strftime_format(format_) == expected


@pytest.mark.parametrize(
    "formats, granularity, value, expected",
    [
        (["yyyyMMdd"], Granularity.DAY, ["20240101"], ["20240102"]),
        (["yyyyMMdd"], Granularity.DAY, ["20240229"], ["20240301"]),
        (["yyyy-MM-dd"], Granularity.DAY, ["2024-12-31"], ["2025-01-01"]),
        (["yyyyMMdd"], Granularity.WEEK, ["20240101"], ["20240108"]),
        (["yyyyMMddHH"], Granularity.HOUR, ["2024010123"], ["2024010200"]),
        (["yyyyMMddHHmm"], Granularity.MINUTE, ["202401010159"], ["202401010200"]),
        (
            ["yyyyMMddHHmmss"],
            Granularity.SECOND,
            ["20240101015959"],
            ["20240101020000"],
        ),
        (["yyyyMM"], Granularity.MONTH, ["202412"], ["202501"]),
        (["yyyyMM"], Granularity.QUARTER, ["202411"], ["202502"]),
        (["yyyy"], Granularity.YEAR, ["2024"], ["2025"]),
        # A month step off the 31st lands on the last day of the next month
        (["yyyyMMdd"], Granularity.MONTH, ["20240131"], ["20240229"]),
        # Multi-column values roll over into the more significant column
        (["yyyyMMdd", "HH"], Granularity.HOUR, ["20240101", "23"], ["20240102", "00"]),
        (["yyyyMMdd", "HH"], Granularity.HOUR, ["20240101", "00"], ["20240101", "01"]),
        # A value that does not match the formats yields no successor
        (["yyyyMMdd"], Granularity.DAY, ["20240101", "00"], None),
        (["yyyyMMdd"], Granularity.DAY, ["not-a-date"], None),
        (["payment_id"], Granularity.DAY, ["12345678"], None),
    ],
)
def test_grain_next_value(formats, granularity, value, expected) -> None:
    """
    Test stepping a partition value forward by one grain.
    """
    grain = PartitionGrain(formats=formats, granularity=granularity)
    assert grain.next_value(value) == expected
