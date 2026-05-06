"""
Tests for ``datajunction_server.mcp.context``.
"""

import pytest

from datajunction_server.mcp import context


def test_get_mcp_session_raises_outside_request() -> None:
    """Tools call this from inside an MCP request handler. Outside that
    context, the ContextVar is unset — must raise loudly."""
    with pytest.raises(RuntimeError) as exc_info:
        context.get_mcp_session()
    assert str(exc_info.value) == (
        "MCP session is not bound — get_mcp_session() can only be called "
        "from inside an MCP tool handler running under mount_mcp."
    )
