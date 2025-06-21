import json
from fastapi.routing import APIRoute
import httpx
import pytest
from fastapi import FastAPI, Request
from unittest.mock import AsyncMock, Mock, patch
from httpx import AsyncClient
from datajunction_server.api.graphql.middleware import (
    GraphQLSessionMiddleware,
    is_mutation,
)


@pytest.mark.asyncio
async def test_middleware_success(module__client: AsyncClient):
    """
    Test that the middleware commits the session and closes it on success.
    """
    mock_session = AsyncMock()
    mock_manager = Mock()
    mock_manager.writer_session = mock_session
    mock_manager.reader_session = mock_session

    with patch(
        "datajunction_server.api.graphql.middleware.get_session_manager",
        return_value=mock_manager,
    ):
        response = await module__client.post(
            "/graphql",
            json={"query": "{ __typename }"},
        )

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
        mock_manager.writer_session = mock_session
        mock_manager.reader_session = mock_session

        with patch(
            "datajunction_server.api.graphql.middleware.get_session_manager",
            return_value=mock_manager,
        ):
            with pytest.raises(ValueError):
                await test_client.post("/graphql", json={"query": "{ __typename }"})
                mock_session.commit.assert_not_called()
                mock_session.rollback.assert_awaited_once()
                mock_session.close.assert_awaited_once()


class TestMutationDetection:
    def encode_body(self, query: str) -> bytes:
        return json.dumps({"query": query}).encode()

    def test_is_mutation_with_valid_mutation(self):
        query = """
        mutation {
            createNode(name: "test") {
                id
            }
        }
        """
        body = self.encode_body(query)
        assert is_mutation(body) is True

    def test_is_mutation_with_query_only(self):
        query = """
        query {
            nodes {
                name
            }
        }
        """
        body = self.encode_body(query)
        assert is_mutation(body) is False

    def test_is_mutation_with_empty_body(self):
        assert is_mutation(b"") is False

    def test_is_mutation_with_no_query_key(self):
        body = json.dumps({"not_query": "blah"}).encode()
        assert is_mutation(body) is False

    def test_is_mutation_with_invalid_json(self):
        assert is_mutation(b"{ this is not json }") is False

    def test_is_mutation_with_invalid_graphql(self):
        bad_query = "blah { this is not valid graphql }"
        body = self.encode_body(bad_query)
        assert is_mutation(body) is False
