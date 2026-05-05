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
- Tools call ``get_mcp_session()`` to read the per-request ``AsyncSession``.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.routing import Mount
from starlette.types import Receive, Scope, Send

from datajunction_server.mcp.context import _session_var, get_mcp_session
from datajunction_server.mcp.server import app as mcp_app
from datajunction_server.utils import get_session_manager

logger = logging.getLogger(__name__)

# Re-export for convenience — tools and tests import from this module.
__all__ = ["mount_mcp", "get_mcp_session"]


def mount_mcp(app: FastAPI, path: str = "/mcp") -> None:
    """Mount the MCP HTTP transport on ``app`` at ``path``.

    Stateless mode: every request is independent (no MCP session tracking
    on the server side). Simpler, scales horizontally, and matches the
    DJ usage pattern where each tool call is a discrete query.

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
    lazy_exit_stack: Optional[AsyncExitStack] = None

    async def ensure_started() -> None:
        nonlocal lazy_started, lazy_exit_stack
        if started_via_lifespan or lazy_started:
            return
        async with lazy_lock:
            if started_via_lifespan or lazy_started:
                return
            stack = AsyncExitStack()
            await stack.enter_async_context(session_manager.run())
            lazy_exit_stack = stack
            lazy_started = True
            logger.info("MCP session manager started lazily (no lifespan)")

    async def asgi_handler(scope: Scope, receive: Receive, send: Send) -> None:
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
                await session_manager.handle_request(scope, receive, send)
            finally:
                _session_var.reset(token)

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
                    yield
            finally:
                started_via_lifespan = False

    app.router.lifespan_context = combined_lifespan
    logger.info("Mounted DJ MCP server at %s", path)
