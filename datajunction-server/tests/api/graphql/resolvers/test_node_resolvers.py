import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datajunction_server.api.graphql.resolvers.nodes import (
    resolve_metrics_and_dimensions,
)
from datajunction_server.api.graphql.scalars.sql import CubeDefinition
from datajunction_server.errors import DJNodeNotFound


@pytest.mark.asyncio
async def test_no_cube_metrics_dimensions_passed():
    session = AsyncMock()
    cube_def = CubeDefinition(
        cube=None,
        metrics=["metric1", "metric2"],
        dimensions=["dim1", "dim2"],
    )

    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube_def)

    assert metrics == ["metric1", "metric2"]
    assert dimensions == ["dim1", "dim2"]


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_cube_by_name")
async def test_cube_found_merges_metrics_and_dimensions(
    mock_get_cube,
    session=AsyncMock(),
):
    cube_def = CubeDefinition(
        cube="my_cube",
        metrics=["metric2", "metric3"],
        dimensions=["dim2", "dim3"],
    )

    # Mock the cube_node with current having cube_node_metrics and cube_node_dimensions
    mock_cube_node = MagicMock()
    mock_cube_node.current.cube_node_metrics = ["metric1", "metric2"]
    mock_cube_node.current.cube_node_dimensions = ["dim1", "dim2"]

    mock_get_cube.return_value = mock_cube_node

    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube_def)

    # Metrics should be cube_node's metrics plus any not duplicated from cube_def.metrics
    assert metrics == ["metric1", "metric2", "metric3"]
    # Dimensions similarly
    assert dimensions == ["dim1", "dim2", "dim3"]


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_cube_by_name")
async def test_cube_not_found_raises(mock_get_cube, session=AsyncMock()):
    cube_def = CubeDefinition(
        cube="missing_cube",
        metrics=["metric1"],
        dimensions=["dim1"],
    )

    mock_get_cube.return_value = None

    with pytest.raises(DJNodeNotFound) as e:
        await resolve_metrics_and_dimensions(session, cube_def)
    assert "missing_cube" in str(e.value)


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_cube_by_name")
async def test_metrics_deduplication(mock_get_cube, session=AsyncMock()):
    cube_def = CubeDefinition(
        cube="cube1",
        metrics=["metric1", "metric2", "metric1", "metric3"],
        dimensions=None,
    )

    mock_cube_node = MagicMock()
    mock_cube_node.current.cube_node_metrics = ["metric2", "metric4"]
    mock_cube_node.current.cube_node_dimensions = []

    mock_get_cube.return_value = mock_cube_node

    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube_def)

    # Deduplication preserves order with cube node metrics first
    assert metrics == ["metric2", "metric4", "metric1", "metric3"]
    assert dimensions == []


@pytest.mark.asyncio
async def test_empty_metrics_and_dimensions_defaults():
    session = AsyncMock()
    cube_def = CubeDefinition(cube=None, metrics=None, dimensions=None)
    metrics, dimensions = await resolve_metrics_and_dimensions(session, cube_def)
    assert metrics == []
    assert dimensions == []
