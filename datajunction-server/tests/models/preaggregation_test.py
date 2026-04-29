"""Tests for pre-aggregation request models."""

from datajunction_server.models.preaggregation import (
    UpdatePreAggregationAvailabilityRequest,
)


def _base_payload(**overrides):
    payload = {
        "catalog": "default",
        "schema": "public",
        "table": "my_preagg",
        "valid_through_ts": 1714329600,
    }
    payload.update(overrides)
    return payload


def test_partition_values_coerced_from_ints():
    """Query services post date partition values as ints (e.g. 20260428).

    The validator must coerce them to strings to satisfy the field's
    ``Optional[List[str]]`` typing.
    """
    req = UpdatePreAggregationAvailabilityRequest(
        **_base_payload(
            min_temporal_partition=[20260101],
            max_temporal_partition=[20260428, 20260429],
        ),
    )
    assert req.min_temporal_partition == ["20260101"]
    assert req.max_temporal_partition == ["20260428", "20260429"]


def test_partition_values_passthrough_when_strings():
    """String partition values must round-trip unchanged."""
    req = UpdatePreAggregationAvailabilityRequest(
        **_base_payload(
            min_temporal_partition=["20260101"],
            max_temporal_partition=["20260428"],
        ),
    )
    assert req.min_temporal_partition == ["20260101"]
    assert req.max_temporal_partition == ["20260428"]


def test_partition_values_none_passthrough():
    """An explicit ``None`` for a partition list must not be coerced."""
    req = UpdatePreAggregationAvailabilityRequest(
        **_base_payload(
            min_temporal_partition=None,
            max_temporal_partition=None,
        ),
    )
    assert req.min_temporal_partition is None
    assert req.max_temporal_partition is None


def test_partition_values_default_empty_list():
    """Omitting the fields keeps the default empty-list factory."""
    req = UpdatePreAggregationAvailabilityRequest(**_base_payload())
    assert req.min_temporal_partition == []
    assert req.max_temporal_partition == []


def test_partition_values_mixed_int_and_str():
    """Heterogeneous lists are normalized to strings element-by-element."""
    req = UpdatePreAggregationAvailabilityRequest(
        **_base_payload(
            min_temporal_partition=[20260101, "20260102"],
        ),
    )
    assert req.min_temporal_partition == ["20260101", "20260102"]
