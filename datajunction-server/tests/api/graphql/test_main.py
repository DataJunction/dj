"""Tests for GraphQL main module."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from datajunction_server.api.graphql.main import get_context


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_production(mock_get_settings, mock_create_loader):
    """Test get_context in production: no test_session attached."""
    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])
    mock_request.app.dependency_overrides = {}

    mock_background_tasks = MagicMock()
    mock_cache = MagicMock()
    mock_settings = MagicMock()
    mock_loader = MagicMock()

    mock_get_settings.return_value = mock_settings
    mock_create_loader.return_value = mock_loader

    context = await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        cache=mock_cache,
    )

    # No shared "session" in context — DataLoaders open their own via
    # session_context() to avoid concurrent-session crashes.
    assert "session" not in context
    assert not hasattr(mock_request.state, "test_session")
    assert context["node_loader"] == mock_loader
    assert context["settings"] == mock_settings
    assert context["request"] == mock_request
    assert context["background_tasks"] == mock_background_tasks
    assert context["cache"] == mock_cache


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_test_override(mock_get_settings, mock_create_loader):
    """Test get_context in tests: shared session is attached from override."""
    from datajunction_server.utils import get_session

    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])
    mock_test_session = AsyncMock()
    mock_request.app.dependency_overrides = {get_session: lambda: mock_test_session}

    mock_background_tasks = MagicMock()
    mock_cache = MagicMock()
    mock_get_settings.return_value = MagicMock()
    mock_create_loader.return_value = MagicMock()

    await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        cache=mock_cache,
    )

    assert mock_request.state.test_session is mock_test_session
