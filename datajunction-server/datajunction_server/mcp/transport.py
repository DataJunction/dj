"""
HTTP transport for the DataJunction MCP server.

Mounts the MCP ``Server`` at ``/mcp`` on the main FastAPI application using
the official Streamable HTTP transport. Each MCP HTTP request opens a
fresh DB session and exposes it to tool implementations through a
``ContextVar``.

Lifecycle:
- ``mount_mcp(app)`` registers the MCP ASGI handler as a Starlette ``Mount``
  and wires the existing FastAPI lifespan to start/stop the MCP session
  manager's task group alongside the rest of the app. As a fallback for
  contexts where the lifespan never fires (e.g. ASGITransport-based tests
  without ``LifespanManager``), the manager is started lazily on the first
  request.
- On lifespan exit the mount stops taking requests and waits up to
  ``drain_timeout`` seconds for in-flight ones to respond.
- Tools call ``get_mcp_session()`` to read the per-request ``AsyncSession``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from contextlib import (
    AbstractContextManager,
    AsyncExitStack,
    asynccontextmanager,
    nullcontext,
)

from fastapi import FastAPI
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.responses import PlainTextResponse
from starlette.routing import Mount
from starlette.types import Receive, Scope, Send

from datajunction_server.mcp.context import _session_var, get_mcp_session
from datajunction_server.mcp.server import app as mcp_app
from datajunction_server.utils import get_session_manager

logger = logging.getLogger(__name__)

# Well under gunicorn's 30s graceful timeout.
DRAIN_TIMEOUT = 5.0

# Re-export for convenience — tools and tests import from this module.
__all__ = ["get_mcp_session", "mount_mcp"]


def mount_mcp(
    app: FastAPI,
    path: str = "/mcp",
    *,
    request_context: Callable[[Scope], AbstractContextManager[object]] | None = None,
    drain_timeout: float = DRAIN_TIMEOUT,
) -> None:
    """Mount the MCP HTTP transport on ``app`` at ``path``.

    Stateless mode: every request is independent (no MCP session tracking
    on the server side). Simpler, scales horizontally, and matches the
    DJ usage pattern where each tool call is a discrete query.

    ``request_context``: optional callable ``(scope) -> contextmanager``. Invoked
    inside the per-request scope (where the DB session is bound) so deployments
    can bind additional request-scoped ContextVars — e.g. an identity-aware
    query-service-client provider. The contextmanager is entered before the MCP
    request is handled and exited (even on error) afterward.

    ``drain_timeout``: seconds to wait on lifespan exit for in-flight requests
    to respond before the session manager tears their streams down.

    Must be called once during app construction.
    """
    session_manager = StreamableHTTPSessionManager(
        app=mcp_app,
        stateless=True,
    )

    # Set up lazily on first request when the FastAPI lifespan didn't fire.
    started_via_lifespan = False
    lazy_started = False
    lazy_lock = asyncio.Lock()
    lazy_exit_stack: AsyncExitStack | None = None

    async def ensure_started() -> None:
        nonlocal lazy_started, lazy_exit_stack
        if started_via_lifespan or lazy_started:
            return
        async with lazy_lock:
            # Re-check inside the lock — defensive against concurrent first
            # requests; only one task should run the manager start-up.
            if started_via_lifespan or lazy_started:  # pragma: no cover
                return
            stack = AsyncExitStack()
            await stack.enter_async_context(session_manager.run())
            lazy_exit_stack = stack
            lazy_started = True
            logger.info("MCP session manager started lazily (no lifespan)")

    # In-flight request tracking, so shutdown can wait them out.
    in_flight = 0
    idle = asyncio.Event()
    idle.set()
    draining = False

    async def handle_scope(scope: Scope, receive: Receive, send: Send) -> None:
        """Wrap MCP request handling with a DB session bound to a ContextVar."""
        if scope["type"] != "http":  # pragma: no cover
            await ensure_started()
            await session_manager.handle_request(scope, receive, send)
            return

        await ensure_started()
        session_factory = get_session_manager().get_writer_session_factory()
        async with session_factory() as session:
            token = _session_var.set(session)
            try:
                cm = (
                    request_context(scope)
                    if request_context is not None
                    else nullcontext()
                )
                with cm:
                    await session_manager.handle_request(scope, receive, send)
            finally:
                _session_var.reset(token)

    async def asgi_handler(scope: Scope, receive: Receive, send: Send) -> None:
        """Count the request, or turn it away while draining."""
        nonlocal in_flight
        if draining:
            response = PlainTextResponse("Server shutting down", status_code=503)
            await response(scope, receive, send)
            return

        in_flight += 1
        idle.clear()
        try:
            await handle_scope(scope, receive, send)
        finally:
            in_flight -= 1
            if in_flight == 0:
                idle.set()

    async def drain_requests() -> None:
        """Wait for in-flight MCP requests to respond."""
        nonlocal draining
        draining = True
        if idle.is_set():
            return
        logger.info("Draining %d MCP requests", in_flight)
        try:
            await asyncio.wait_for(idle.wait(), drain_timeout)
        except TimeoutError:
            logger.warning("Dropping %d MCP requests still in flight", in_flight)

    app.router.routes.append(Mount(path, app=asgi_handler))

    # Compose the MCP manager's task-group lifespan with whatever the app
    # already has, so a single lifespan covers both.
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def combined_lifespan(_app: FastAPI):
        nonlocal started_via_lifespan
        async with session_manager.run():
            started_via_lifespan = True
            try:
                async with original_lifespan(_app):
                    try:
                        yield
                    finally:
                        await drain_requests()
            finally:
                started_via_lifespan = False

    app.router.lifespan_context = combined_lifespan
    logger.info("Mounted DJ MCP server at %s", path)
