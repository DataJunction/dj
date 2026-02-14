"""Tests for GraphQL main module."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from datajunction_server.api.graphql.main import get_context


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_without_test_session(mock_get_settings, mock_create_loader):
    """Test get_context when request.state doesn't have test_session attribute"""
    # Create a mock request without test_session
    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])  # Empty spec means no attributes initially

    mock_background_tasks = MagicMock()
    mock_db_session = AsyncMock()
    mock_cache = MagicMock()
    mock_settings = MagicMock()
    mock_loader = MagicMock()

    mock_get_settings.return_value = mock_settings
    mock_create_loader.return_value = mock_loader

    # Call get_context
    context = await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        db_session=mock_db_session,
        cache=mock_cache,
    )

    # Verify test_session was set on request.state
    assert hasattr(mock_request.state, "test_session")
    assert mock_request.state.test_session == mock_db_session

    # Verify context contains expected keys
    assert context["session"] == mock_db_session
    assert context["node_loader"] == mock_loader
    assert context["settings"] == mock_settings
    assert context["request"] == mock_request
    assert context["background_tasks"] == mock_background_tasks
    assert context["cache"] == mock_cache


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_with_existing_test_session(
    mock_get_settings,
    mock_create_loader,
):
    """Test get_context when request.state already has test_session attribute"""
    # Create a mock request WITH test_session already set
    mock_request = MagicMock()
    existing_test_session = AsyncMock()
    mock_request.state.test_session = existing_test_session

    mock_background_tasks = MagicMock()
    mock_db_session = AsyncMock()
    mock_cache = MagicMock()
    mock_settings = MagicMock()
    mock_loader = MagicMock()

    mock_get_settings.return_value = mock_settings
    mock_create_loader.return_value = mock_loader

    # Call get_context
    context = await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        db_session=mock_db_session,
        cache=mock_cache,
    )

    # Verify test_session was NOT overwritten - should still be the existing one
    assert mock_request.state.test_session == existing_test_session

    # Verify context still uses the db_session passed in (not test_session)
    assert context["session"] == mock_db_session
    assert context["node_loader"] == mock_loader
    assert context["settings"] == mock_settings
    assert context["request"] == mock_request
    assert context["background_tasks"] == mock_background_tasks
    assert context["cache"] == mock_cache
