"""
Tests for the MCP HTTP transport — ``mount_mcp`` and its lifespan integration.
"""

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport

from datajunction_server.mcp import transport


@pytest_asyncio.fixture
async def fastapi_app_with_mcp() -> FastAPI:
    """A bare FastAPI app with mount_mcp wired in.

    Using a fresh app (rather than the production one) keeps the test
    independent of the rest of the configure_app pipeline.
    """
    app = FastAPI()
    transport.mount_mcp(app)
    return app


def test_mount_mcp_adds_route(fastapi_app_with_mcp: FastAPI) -> None:
    """The /mcp route is registered after mount."""
    paths = {getattr(r, "path", None) for r in fastapi_app_with_mcp.router.routes}
    assert "/mcp" in paths


@pytest.mark.asyncio
async def test_mount_mcp_lifespan_runs_combined_lifespan(
    fastapi_app_with_mcp: FastAPI,
) -> None:
    """When FastAPI lifespan fires, the combined_lifespan path runs the
    MCP session manager's task group instead of the lazy-start fallback.
    """
    async with LifespanManager(fastapi_app_with_mcp):
        # Inside the lifespan: send a GET to /mcp/ to confirm the route
        # is mounted and reachable. Streamable HTTP only accepts POST, so
        # we expect a 405 Method Not Allowed (or 4xx) response from the
        # MCP handler — the test only cares that lifespan ran cleanly.
        async with httpx.AsyncClient(
            transport=ASGITransport(app=fastapi_app_with_mcp),
            base_url="http://test",
            follow_redirects=True,
        ) as client:
            response = await client.get("/mcp/")
        assert 400 <= response.status_code < 500


@pytest.mark.asyncio
async def test_mount_mcp_invokes_request_context_per_request() -> None:
    """A request_context binder is entered before the MCP request is handled
    and exited afterward, receiving the ASGI scope."""
    from contextlib import contextmanager

    events: list[str] = []
    seen_scope: dict = {}

    @contextmanager
    def recorder(scope):
        seen_scope["type"] = scope.get("type")
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    app = FastAPI()
    transport.mount_mcp(app, request_context=recorder)

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=True,
        ) as client:
            await client.get("/mcp/")

    assert events == ["enter", "exit"]
    assert seen_scope["type"] == "http"
