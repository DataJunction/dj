"""Tests for GraphQL DataLoaders."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.api.graphql.dataloaders import (
    batch_load_nodes,
    batch_load_nodes_by_name_only,
    create_node_by_name_loader,
)


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_with_fields(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes with specific fields requested"""
    # Setup mocks
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"
    mock_node2 = MagicMock()
    mock_node2.name = "default.node2"

    mock_find_by.return_value = [mock_node1, mock_node2]
    mock_load_options.return_value = ["option1", "option2"]

    mock_request = MagicMock()

    # Test with multiple nodes and different field selections
    keys = [
        ("default.node1", {"name": None, "current": {"displayName": None}}),
        ("default.node2", {"name": None, "type": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify all_fields was merged correctly
    expected_all_fields = {
        "name": None,
        "current": {"displayName": None},
        "type": None,
    }
    mock_load_options.assert_called_once_with(expected_all_fields)

    # Verify find_by was called with correct parameters
    mock_find_by.assert_called_once_with(
        mock_session,
        names=["default.node1", "default.node2"],
        options=["option1", "option2"],
    )

    # Verify results are in correct order
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] == mock_node2


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_with_no_fields(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes with no fields (None) in all keys"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]

    mock_request = MagicMock()

    # All keys have None for fields
    keys = [
        ("default.node1", None),
        ("default.node2", None),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify load_node_options was NOT called since all_fields is empty
    mock_load_options.assert_not_called()

    # Verify find_by was called with empty options
    mock_find_by.assert_called_once_with(
        mock_session,
        names=["default.node1", "default.node2"],
        options=[],
    )

    # Verify result contains the found node and None for the missing one
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] is None


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_missing_nodes(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes returns None for missing nodes"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    # Only node1 exists in DB
    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    # Request 3 nodes but only 1 exists
    keys = [
        ("default.node1", {"name": None}),
        ("default.missing_node", {"name": None}),
        ("default.another_missing", {"name": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify results are in correct order with None for missing nodes
    assert len(result) == 3
    assert result[0] == mock_node1
    assert result[1] is None
    assert result[2] is None


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_ordering_preservation(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes preserves the order of requested keys"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.aaa"
    mock_node2 = MagicMock()
    mock_node2.name = "default.zzz"
    mock_node3 = MagicMock()
    mock_node3.name = "default.mmm"

    # Return nodes in different order than requested
    mock_find_by.return_value = [mock_node2, mock_node1, mock_node3]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    # Request in specific order
    keys = [
        ("default.zzz", {"name": None}),
        ("default.aaa", {"name": None}),
        ("default.mmm", {"name": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify results match the requested order (not DB return order)
    assert len(result) == 3
    assert result[0].name == "default.zzz"
    assert result[1].name == "default.aaa"
    assert result[2].name == "default.mmm"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_by_name_only(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes_by_name_only with default fields"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"
    mock_node2 = MagicMock()
    mock_node2.name = "default.node2"

    mock_find_by.return_value = [mock_node1, mock_node2]
    mock_load_options.return_value = ["default_options"]

    mock_request = MagicMock()

    names = ["default.node1", "default.node2"]

    result = await batch_load_nodes_by_name_only(names, mock_request)

    # Verify load_node_options was called with default fields
    mock_load_options.assert_called_once_with({"name": None, "current": {"name": None}})

    # Verify find_by was called correctly
    mock_find_by.assert_called_once_with(
        mock_session,
        names=names,
        options=["default_options"],
    )

    # Verify results
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] == mock_node2


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_by_name_only_with_missing(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes_by_name_only returns None for missing nodes"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    names = ["default.node1", "default.missing", "default.node3"]

    result = await batch_load_nodes_by_name_only(names, mock_request)

    # Verify results with None for missing
    assert len(result) == 3
    assert result[0] == mock_node1
    assert result[1] is None
    assert result[2] is None


@patch("datajunction_server.api.graphql.dataloaders.DataLoader")
def test_create_node_by_name_loader(mock_dataloader):
    """Test create_node_by_name_loader creates DataLoader with correct load_fn"""
    mock_request = MagicMock()
    mock_loader_instance = MagicMock()
    mock_dataloader.return_value = mock_loader_instance

    result = create_node_by_name_loader(mock_request)

    # Verify DataLoader was called
    assert mock_dataloader.called
    call_kwargs = mock_dataloader.call_args[1]

    # Verify load_fn is set correctly
    assert "load_fn" in call_kwargs
    assert callable(call_kwargs["load_fn"])

    # Verify result is the DataLoader instance
    assert result == mock_loader_instance
