"""
Per-request context for MCP tool handlers.

Holds a ``ContextVar`` for the DB session bound to the current MCP HTTP
request. Lives in its own module to break the
``transport`` → ``server`` → ``tools`` → ``transport`` import cycle.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession


_session_var: ContextVar[Optional[AsyncSession]] = ContextVar(
    "dj_mcp_session",
    default=None,
)


def get_mcp_session() -> AsyncSession:
    """Return the DB session bound to the current MCP request.

    Tools call this to access the database without taking the session as
    a parameter (the MCP SDK doesn't pass per-request context to tool
    handlers).
    """
    session = _session_var.get()
    if session is None:
        raise RuntimeError(
            "MCP session is not bound — get_mcp_session() can only be called "
            "from inside an MCP tool handler running under mount_mcp.",
        )
    return session
