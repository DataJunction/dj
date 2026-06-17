"""Tests for models."""

from datajunction.models import (
    MetricDirection,
    MetricMetadata,
    MetricUnit,
    QueryState,
)


def test_metric_metadata_from_dict():
    """
    Check that MetricMetadata deserializes a server payload, normalizing the
    direction/unit casing and reading the nested unit name.
    """
    metadata = MetricMetadata.from_dict(
        dj_client=None,
        data={
            "direction": "HIGHER_IS_BETTER",
            "unit": {"name": "DOLLAR"},
            "significant_digits": 2,
            "min_decimal_exponent": -3,
            "max_decimal_exponent": 5,
        },
    )
    assert metadata == MetricMetadata(
        direction=MetricDirection.HIGHER_IS_BETTER,
        unit=MetricUnit.DOLLAR,
        significant_digits=2,
        min_decimal_exponent=-3,
        max_decimal_exponent=5,
    )


def test_enum_list():
    """
    Check list of query states works
    """
    assert QueryState.list() == [
        "UNKNOWN",
        "ACCEPTED",
        "SCHEDULED",
        "RUNNING",
        "FINISHED",
        "CANCELED",
        "FAILED",
    ]
