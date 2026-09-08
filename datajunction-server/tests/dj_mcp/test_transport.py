"""
Tests for the MCP HTTP transport — ``mount_mcp`` and its lifespan integration.
"""

import asyncio

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

    async with (
        LifespanManager(app),
        httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=True,
        ) as client,
    ):
        await client.get("/mcp/")

    assert events == ["enter", "exit"]
    assert seen_scope["type"] == "http"


@pytest.mark.asyncio
async def test_shutdown_drains_in_flight(monkeypatch) -> None:
    """Lifespan shutdown waits for every in-flight MCP request to respond."""
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.responses import PlainTextResponse

    events: list[str] = []
    entered: list[str] = []

    async def slow_handler(_self, scope, receive, send):
        wait = scope["query_string"].decode().removeprefix("wait=")
        entered.append(wait)
        await asyncio.sleep(float(wait))
        await PlainTextResponse("ok")(scope, receive, send)
        events.append(wait)

    monkeypatch.setattr(StreamableHTTPSessionManager, "handle_request", slow_handler)

    app = FastAPI()
    transport.mount_mcp(app)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        requests = [
            asyncio.create_task(client.post(f"/mcp/?wait={wait}"))
            for wait in ("0.05", "0.2")
        ]
        while len(entered) < 2:
            await asyncio.sleep(0.01)
        await lifespan.__aexit__(None, None, None)
        events.append("lifespan done")
        responses = await asyncio.gather(*requests)

    assert events == ["0.05", "0.2", "lifespan done"]
    assert [(r.status_code, r.text) for r in responses] == [(200, "ok"), (200, "ok")]


@pytest.mark.asyncio
async def test_drain_gives_up_after_timeout(monkeypatch) -> None:
    """Shutdown stops waiting once the drain timeout passes."""
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.responses import PlainTextResponse

    events: list[str] = []
    started = asyncio.Event()

    async def slow_handler(_self, scope, receive, send):
        started.set()
        await asyncio.sleep(0.3)
        await PlainTextResponse("ok")(scope, receive, send)
        events.append("request done")

    monkeypatch.setattr(StreamableHTTPSessionManager, "handle_request", slow_handler)

    app = FastAPI()
    transport.mount_mcp(app, drain_timeout=0.01)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        request = asyncio.create_task(client.post("/mcp/"))
        await started.wait()
        await lifespan.__aexit__(None, None, None)
        events.append("lifespan done")
        await request

    assert events == ["lifespan done", "request done"]


@pytest.mark.asyncio
async def test_drain_rejects_new_requests(monkeypatch) -> None:
    """While draining, a new MCP request gets 503 instead of a dropped stream."""
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.responses import PlainTextResponse

    started = asyncio.Event()
    release = asyncio.Event()

    async def blocking_handler(_self, scope, receive, send):
        started.set()
        await release.wait()
        await PlainTextResponse("ok")(scope, receive, send)

    monkeypatch.setattr(
        StreamableHTTPSessionManager,
        "handle_request",
        blocking_handler,
    )

    app = FastAPI()
    transport.mount_mcp(app, drain_timeout=0.01)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=True,
    ) as client:
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        first = asyncio.create_task(client.post("/mcp/"))
        await started.wait()
        await lifespan.__aexit__(None, None, None)
        late = await client.post("/mcp/")
        release.set()
        first_response = await first

    assert late.status_code == 503
    assert late.text == "Server shutting down"
    assert first_response.status_code == 200
    assert first_response.text == "ok"


@pytest.mark.asyncio
async def test_mount_mcp_request_context_exits_on_handler_error(monkeypatch) -> None:
    """If MCP request handling raises, the request_context is still exited."""
    from contextlib import contextmanager

    events: list[str] = []

    @contextmanager
    def recorder(scope):
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    app = FastAPI()
    transport.mount_mcp(app, request_context=recorder)

    async with LifespanManager(app):
        # Patch the mounted handler so request handling raises after the
        # binder is entered.
        async def boom(*_a, **_k):
            raise RuntimeError("boom")

        # The Mount's app is the asgi_handler closure; patch the session
        # manager's handle_request which it awaits.
        from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

        monkeypatch.setattr(StreamableHTTPSessionManager, "handle_request", boom)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            follow_redirects=True,
        ) as client:
            try:
                await client.get("/mcp/")
            except Exception:
                pass  # the handler raising may surface as a transport error

    assert events == ["enter", "exit"]
