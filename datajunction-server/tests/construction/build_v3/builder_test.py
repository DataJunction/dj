"""Unit tests for helpers in ``construction.build_v3.builder``."""

from datajunction_server.construction.build_v3 import builder


def test_telemetry_filter_columns_none_and_empty():
    """No filters yields no columns."""
    assert builder._telemetry_filter_columns(None) == []
    assert builder._telemetry_filter_columns([]) == []


def test_telemetry_filter_columns_extracts_and_dedupes():
    """Column references are extracted and de-duplicated across predicates,
    and literal values are never included."""
    columns = builder._telemetry_filter_columns(
        [
            "default.hard_hat.state = 'AZ'",
            "default.hard_hat.state <> 'NY' AND default.repair_order.hard_hat_id > 5",
        ],
    )
    assert columns == ["default.hard_hat.state", "default.repair_order.hard_hat_id"]
    assert "AZ" not in columns and "NY" not in columns
