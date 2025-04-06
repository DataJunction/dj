"""Tests for models."""

from datajunction.models import QueryState


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
