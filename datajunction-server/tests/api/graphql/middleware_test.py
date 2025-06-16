from fastapi.routing import APIRoute
import pytest
from fastapi import Request
from unittest.mock import AsyncMock, Mock, patch


@pytest.mark.asyncio
async def test_middleware_success(client):
    """
    Test that the middleware commits the session and closes it on success.
    """
    mock_session = AsyncMock()
    mock_manager = Mock()
    mock_manager.session = Mock(return_value=mock_session)

    with patch(
        "datajunction_server.api.graphql.middleware.get_session_manager",
        return_value=mock_manager,
    ):
        response = await client.post("/graphql", json={"query": "{ __typename }"})

    assert response.status_code == 200
    mock_session.commit.assert_awaited_once()
    mock_session.rollback.assert_not_called()
    mock_session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_failure(client):
    """
    Test that the middleware rolls back the session and closes it on failure.
    """

    async def failing_endpoint(request: Request):
        raise ValueError("Something went wrong")

    app = client.app
    app.router.routes = [
        route
        for route in app.router.routes
        if not (hasattr(route, "path") and route.path == "/graphql")
    ]
    app.router.routes.append(APIRoute("/graphql", failing_endpoint, methods=["POST"]))

    mock_session = AsyncMock()
    mock_manager = Mock()
    mock_manager.session = Mock(return_value=mock_session)

    with patch(
        "datajunction_server.api.graphql.middleware.get_session_manager",
        return_value=mock_manager,
    ):
        with pytest.raises(ValueError):
            await client.post("/graphql", json={"query": "{ __typename }"})

    mock_session.commit.assert_not_called()
    mock_session.rollback.assert_awaited_once()
    mock_session.close.assert_awaited_once()
