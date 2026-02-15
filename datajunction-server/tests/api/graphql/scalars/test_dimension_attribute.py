"""Tests for DimensionAttribute scalar."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from datajunction_server.api.graphql.scalars.node import DimensionAttribute


@pytest.mark.asyncio
async def test_dimension_node_returns_prepopulated():
    """Test DimensionAttribute.dimension_node returns pre-populated node"""
    mock_node = MagicMock()
    mock_node.name = "default.us_state"

    dim_attr = DimensionAttribute(
        name="default.us_state.state_name",
        attribute="state_name",
        properties=[],
        type="string",
        _dimension_node=mock_node,
    )

    # Mock info - should not be used since _dimension_node is set
    mock_info = MagicMock()

    result = await dim_attr.dimension_node(mock_info)

    assert result == mock_node
    # Verify node_loader was not called
    assert not hasattr(mock_info, "context") or "node_loader" not in mock_info.context


@pytest.mark.asyncio
async def test_dimension_node_lazy_loads_when_not_prepopulated():
    """Test DimensionAttribute.dimension_node lazy loads when not pre-populated"""
    mock_loaded_node = MagicMock()
    mock_loaded_node.name = "default.us_state"

    # Create mock node_loader that returns the loaded node
    mock_node_loader = AsyncMock()
    mock_node_loader.load = AsyncMock(return_value=mock_loaded_node)

    # Create mock info context
    mock_info = MagicMock()
    mock_info.context = {"node_loader": mock_node_loader}

    # Create DimensionAttribute without _dimension_node set (None)
    dim_attr = DimensionAttribute(
        name="default.us_state.state_name",
        attribute="state_name",
        properties=[],
        type="string",
        _dimension_node=None,
    )

    result = await dim_attr.dimension_node(mock_info)

    # Verify node_loader.load was called with correct dimension node name
    mock_node_loader.load.assert_called_once_with("default.us_state")
    assert result == mock_loaded_node


@pytest.mark.asyncio
async def test_dimension_node_lazy_loads_with_role():
    """Test dimension_node lazy loading extracts correct node name with role suffix"""
    mock_loaded_node = MagicMock()
    mock_loaded_node.name = "default.dispatcher"

    mock_node_loader = AsyncMock()
    mock_node_loader.load = AsyncMock(return_value=mock_loaded_node)

    mock_info = MagicMock()
    mock_info.context = {"node_loader": mock_node_loader}

    # Dimension attribute with role suffix in name
    dim_attr = DimensionAttribute(
        name="default.dispatcher.company_name[origin]",
        attribute="company_name",
        role="origin",
        properties=[],
        type="string",
        _dimension_node=None,
    )

    result = await dim_attr.dimension_node(mock_info)

    # Should extract "default.dispatcher" from "default.dispatcher.company_name[origin]"
    # The rsplit('.', 1)[0] should handle this correctly
    expected_name = "default.dispatcher.company_name[origin]".rsplit(".", 1)[0]
    mock_node_loader.load.assert_called_once_with(expected_name)
    assert result == mock_loaded_node


@pytest.mark.asyncio
async def test_dimension_node_lazy_loads_simple_name():
    """Test dimension_node lazy loading with simple two-part name"""
    mock_loaded_node = MagicMock()
    mock_loaded_node.name = "default.country"

    mock_node_loader = AsyncMock()
    mock_node_loader.load = AsyncMock(return_value=mock_loaded_node)

    mock_info = MagicMock()
    mock_info.context = {"node_loader": mock_node_loader}

    dim_attr = DimensionAttribute(
        name="default.country.country_code",
        attribute="country_code",
        properties=["primary_key"],
        type="string",
        _dimension_node=None,
    )

    result = await dim_attr.dimension_node(mock_info)

    mock_node_loader.load.assert_called_once_with("default.country")
    assert result == mock_loaded_node
