"""
Per-request context for MCP tool handlers.

Holds a ``ContextVar`` for the DB session bound to the current MCP HTTP
request. Lives in its own module to break the
``transport`` → ``server`` → ``tools`` → ``transport`` import cycle.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Callable, Optional

from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from datajunction_server.service_clients import QueryServiceClient


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


_qsc_provider_var: ContextVar[Optional[Callable[[], "QueryServiceClient"]]] = ContextVar(
    "dj_mcp_qsc_provider",
    default=None,
)


def get_mcp_query_service_client() -> Optional["QueryServiceClient"]:
    """Return a request-scoped query-service client if a provider is bound.

    Deployments bind a provider via ``mount_mcp(request_context=...)`` that
    returns an identity-aware, TLS-configured client. Returns ``None`` when no
    provider is bound, so callers fall back to the default OSS factory. A
    fail-closed provider may raise — the error propagates to the tool handler.
    """
    provider = _qsc_provider_var.get()
    return provider() if provider is not None else None
