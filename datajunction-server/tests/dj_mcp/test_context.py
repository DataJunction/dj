"""
Tests for ``datajunction_server.mcp.context``.
"""

from typing import cast

import pytest

from datajunction_server.mcp import context
from datajunction_server.service_clients import QueryServiceClient


def test_get_mcp_query_service_client_none_when_unbound() -> None:
    """No provider bound → returns None so callers fall back to the OSS factory."""
    assert context.get_mcp_query_service_client() is None


def test_get_mcp_query_service_client_calls_provider_when_bound() -> None:
    """When a provider is bound, it is invoked and its client returned."""
    sentinel = cast(QueryServiceClient, object())
    token = context._qsc_provider_var.set(lambda: sentinel)
    try:
        assert context.get_mcp_query_service_client() is sentinel
    finally:
        context._qsc_provider_var.reset(token)


def test_get_mcp_query_service_client_propagates_provider_error() -> None:
    """A fail-closed provider that raises must surface to the caller."""

    def _boom():
        raise ValueError("no caller identity")

    token = context._qsc_provider_var.set(_boom)
    try:
        with pytest.raises(ValueError, match="no caller identity"):
            context.get_mcp_query_service_client()
    finally:
        context._qsc_provider_var.reset(token)


def test_get_mcp_session_raises_outside_request() -> None:
    """Tools call this from inside an MCP request handler. Outside that
    context, the ContextVar is unset — must raise loudly."""
    with pytest.raises(RuntimeError) as exc_info:
        context.get_mcp_session()
    assert str(exc_info.value) == (
        "MCP session is not bound — get_mcp_session() can only be called "
        "from inside an MCP tool handler running under mount_mcp."
    )
