from fastapi.routing import APIRoute
import httpx
import pytest
from fastapi import FastAPI, Request
from unittest.mock import AsyncMock, Mock, patch
from datajunction_server.api.graphql.middleware import GraphQLSessionMiddleware


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
async def test_middleware_failure():
    """
    Test that the middleware rolls back the session and closes it on failure.
    """

    async def failing_endpoint(request: Request):
        raise ValueError("Something went wrong")

    app = FastAPI(
        routes=[APIRoute("/graphql", endpoint=failing_endpoint, methods=["POST"])],
    )
    app.add_middleware(GraphQLSessionMiddleware)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        mock_session = AsyncMock()
        mock_manager = Mock()
        mock_manager.session = Mock(return_value=mock_session)

        with patch(
            "datajunction_server.api.graphql.middleware.get_session_manager",
            return_value=mock_manager,
        ):
            with pytest.raises(ValueError):
                await test_client.post("/graphql", json={"query": "{ __typename }"})
                mock_session.commit.assert_not_called()
                mock_session.rollback.assert_awaited_once()
                mock_session.close.assert_awaited_once()
