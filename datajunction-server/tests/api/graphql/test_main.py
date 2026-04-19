"""Tests for GraphQL main module."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from datajunction_server.api.graphql.main import get_context


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context(mock_get_settings, mock_create_loader):
    """Test get_context returns expected keys without a shared session."""
    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])

    mock_background_tasks = MagicMock()
    mock_db_session = AsyncMock()
    mock_cache = MagicMock()
    mock_settings = MagicMock()
    mock_loader = MagicMock()

    mock_get_settings.return_value = mock_settings
    mock_create_loader.return_value = mock_loader

    context = await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        db_session=mock_db_session,
        cache=mock_cache,
    )

    # No shared "session" in context — resolvers create their own via
    # resolver_session() to avoid concurrent-session crashes.
    assert "session" not in context
    assert context["node_loader"] == mock_loader
    assert context["settings"] == mock_settings
    assert context["request"] == mock_request
    assert context["background_tasks"] == mock_background_tasks
    assert context["cache"] == mock_cache
