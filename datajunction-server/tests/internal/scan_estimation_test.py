"""
Tests for scan estimation functionality.
"""

from unittest.mock import MagicMock


from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.internal.scan_estimation import calculate_scan_estimate
from datajunction_server.models.sql import ScanEstimate


def test_calculate_scan_estimate_empty_physical_tables():
    """
    Test that calculate_scan_estimate returns None when physical_tables is empty.
    """
    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
    )

    result = calculate_scan_estimate([], ctx)

    assert result is None


def test_calculate_scan_estimate_missing_node():
    """
    Test scan estimation when a source node doesn't exist in ctx.nodes.

    This covers the edge case: if not node
    """
    # Setup: Create a BuildContext with an empty nodes dict
    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={},  # Empty - source won't be found
    )

    physical_tables = ["missing.source.table"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert isinstance(result, ScanEstimate)
    assert result.total_bytes is None  # No size data available
    assert len(result.sources) == 1

    source_info = result.sources[0]
    assert source_info.source_name == "missing.source.table"
    assert source_info.total_bytes is None
    assert source_info.partition_columns == []
    assert source_info.total_partition_count is None
    assert source_info.catalog is None
    assert source_info.schema_ is None
    assert source_info.table is None


def test_calculate_scan_estimate_node_without_current():
    """
    Test scan estimation when a node exists but has no current revision.

    This covers the edge case: if not node.current
    """
    # Setup: Create a mock node with no current revision
    # Note: Don't use spec=Node to avoid SQLAlchemy operator issues
    mock_node = MagicMock()
    mock_node.current = None  # Key: node exists but current is None

    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={"my.source.table": mock_node},
    )

    physical_tables = ["my.source.table"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert isinstance(result, ScanEstimate)
    assert result.total_bytes is None
    assert len(result.sources) == 1

    source_info = result.sources[0]
    assert source_info.source_name == "my.source.table"
    assert source_info.total_bytes is None
    assert source_info.partition_columns == []
    assert source_info.total_partition_count is None


def test_calculate_scan_estimate_mixed_missing_and_no_current():
    """
    Test scan estimation with a mix of:
    - Missing nodes (not in ctx.nodes)
    - Nodes without current revision (node.current is None)

    Both should result in SourceScanInfo entries with None metadata.
    """
    # Setup: Create mixed scenario
    mock_node_no_current = MagicMock()
    mock_node_no_current.current = None

    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={"node.without.current": mock_node_no_current},
        # "completely.missing" is not in nodes dict
    )

    physical_tables = ["completely.missing", "node.without.current"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert result.total_bytes is None  # All sources have no size data
    assert len(result.sources) == 2

    # Both should have None metadata
    for source_info in result.sources:
        assert source_info.total_bytes is None
        assert source_info.partition_columns == []
        assert source_info.total_partition_count is None

    # Check specific source names
    source_names = {s.source_name for s in result.sources}
    assert "completely.missing" in source_names
    assert "node.without.current" in source_names


def test_calculate_scan_estimate_node_with_availability():
    """
    Test scan estimation with a valid node that has availability data.

    This ensures the edge case handling doesn't break normal flow.
    """
    # Setup: Create a node with current revision and availability
    mock_availability = MagicMock()
    mock_availability.total_size_bytes = 1024000
    mock_availability.catalog = "test_catalog"
    mock_availability.schema_ = "test_schema"
    mock_availability.table = "test_table"
    mock_availability.temporal_partitions = ["date"]
    mock_availability.total_partitions = 100

    mock_revision = MagicMock()
    mock_revision.availability = mock_availability

    mock_node = MagicMock()
    mock_node.current = mock_revision

    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={"valid.source.table": mock_node},
    )

    physical_tables = ["valid.source.table"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert result.total_bytes == 1024000
    assert len(result.sources) == 1

    source_info = result.sources[0]
    assert source_info.source_name == "valid.source.table"
    assert source_info.total_bytes == 1024000
    assert source_info.catalog == "test_catalog"
    assert source_info.schema_ == "test_schema"
    assert source_info.table == "test_table"
    assert source_info.partition_columns == ["date"]
    assert source_info.total_partition_count == 100


def test_calculate_scan_estimate_mixed_valid_and_invalid():
    """
    Test scan estimation with a mix of valid and invalid nodes.

    - One valid node with availability data
    - One missing node
    - One node without current revision

    Should aggregate total_bytes only from valid nodes.
    """
    # Setup valid node
    mock_availability = MagicMock()
    mock_availability.total_size_bytes = 500000
    mock_availability.catalog = "catalog1"
    mock_availability.schema_ = "schema1"
    mock_availability.table = "table1"
    mock_availability.temporal_partitions = []
    mock_availability.total_partitions = None

    mock_revision = MagicMock()
    mock_revision.availability = mock_availability

    mock_valid_node = MagicMock()
    mock_valid_node.current = mock_revision

    # Setup node without current
    mock_invalid_node = MagicMock()
    mock_invalid_node.current = None

    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={
            "valid.table": mock_valid_node,
            "invalid.table": mock_invalid_node,
            # "missing.table" not in nodes
        },
    )

    physical_tables = ["valid.table", "missing.table", "invalid.table"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert result.total_bytes == 500000  # Only from valid node
    assert len(result.sources) == 3

    # Find each source and verify
    sources_by_name = {s.source_name: s for s in result.sources}

    # Valid source
    valid_source = sources_by_name["valid.table"]
    assert valid_source.total_bytes == 500000
    assert valid_source.catalog == "catalog1"

    # Missing source
    missing_source = sources_by_name["missing.table"]
    assert missing_source.total_bytes is None
    assert missing_source.catalog is None

    # Invalid source (no current)
    invalid_source = sources_by_name["invalid.table"]
    assert invalid_source.total_bytes is None
    assert invalid_source.catalog is None


def test_calculate_scan_estimate_node_with_no_availability():
    """
    Test scan estimation when node.current exists but availability is None.

    This should result in 0 bytes for that source.
    """
    mock_revision = MagicMock()
    mock_revision.availability = None  # No availability data

    mock_node = MagicMock()
    mock_node.current = mock_revision

    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={"source.no.availability": mock_node},
    )

    physical_tables = ["source.no.availability"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert result.total_bytes is None  # 0 bytes results in None
    assert len(result.sources) == 1

    source_info = result.sources[0]
    assert source_info.source_name == "source.no.availability"
    assert source_info.total_bytes == 0  # 0 when availability is None
    assert source_info.catalog is None
    assert source_info.partition_columns == []


def test_calculate_scan_estimate_use_materialized_flag():
    """
    Test that the use_materialized flag is properly reflected in the result.
    """
    ctx = BuildContext(
        session=MagicMock(),
        metrics=[],
        dimensions=[],
        nodes={},
        use_materialized=True,
    )

    physical_tables = ["some.table"]

    # Execute
    result = calculate_scan_estimate(physical_tables, ctx)

    # Assert
    assert result is not None
    assert result.has_materialization is True

    # Test with use_materialized=False
    ctx.use_materialized = False
    result = calculate_scan_estimate(physical_tables, ctx)

    assert result is not None
    assert result.has_materialization is False
