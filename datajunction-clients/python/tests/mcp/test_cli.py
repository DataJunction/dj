"""
Tests for the dj-mcp stdio→HTTP proxy CLI.

The CLI delegates everything to a hosted DJ MCP server via Streamable HTTP.
We just verify the wiring: settings → headers, upstream connect, stdio
bridge runs. Actual tool behavior is tested in datajunction-server's
``tests/dj_mcp/`` suite.
"""

from unittest.mock import AsyncMock, MagicMock

import mcp.types as types
import pytest
from mcp.server import Server

from datajunction.mcp import cli


def test_dj_api_url_backwards_compat(monkeypatch) -> None:
    """Pre-migration deployments set ``DJ_API_URL``; the new CLI honours
    it and appends ``/mcp`` so existing setups don't silently break."""
    from datajunction.mcp.config import get_mcp_settings

    monkeypatch.delenv("DJ_MCP_URL", raising=False)
    monkeypatch.setenv("DJ_API_URL", "https://dj.example.com")
    assert get_mcp_settings().mcp_url == "https://dj.example.com/mcp"


def test_dj_mcp_url_takes_precedence_over_dj_api_url(monkeypatch) -> None:
    """If both are set, ``DJ_MCP_URL`` wins — no surprise when explicitly configured."""
    from datajunction.mcp.config import get_mcp_settings

    monkeypatch.setenv("DJ_MCP_URL", "https://override/mcp")
    monkeypatch.setenv("DJ_API_URL", "https://legacy")
    assert get_mcp_settings().mcp_url == "https://override/mcp"


def test_default_mcp_url_when_neither_set(monkeypatch) -> None:
    """No env vars → localhost default."""
    from datajunction.mcp.config import get_mcp_settings

    monkeypatch.delenv("DJ_MCP_URL", raising=False)
    monkeypatch.delenv("DJ_API_URL", raising=False)
    assert get_mcp_settings().mcp_url == "http://localhost:8000/mcp"


@pytest.mark.asyncio
async def test_main_connects_with_bearer_token_when_set(monkeypatch):
    """When DJ_API_TOKEN is set, it's forwarded as an Authorization header."""
    monkeypatch.setenv("DJ_API_TOKEN", "test-token")
    monkeypatch.setenv("DJ_MCP_URL", "http://example.com/mcp")

    captured: dict = {}

    class _FakeStreamCtx:
        async def __aenter__(self):
            return (MagicMock(), MagicMock(), lambda: None)

        async def __aexit__(self, *exc):
            return False

    def fake_streamablehttp_client(url, headers=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        return _FakeStreamCtx()

    class _FakeSessionCtx:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            session = MagicMock()
            session.initialize = AsyncMock()
            return session

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "streamablehttp_client", fake_streamablehttp_client)
    monkeypatch.setattr(cli, "ClientSession", _FakeSessionCtx)
    monkeypatch.setattr(cli, "_serve", AsyncMock())

    await cli.main()

    assert captured["url"] == "http://example.com/mcp"
    assert captured["headers"] == {"Authorization": "Bearer test-token"}


@pytest.mark.asyncio
async def test_main_without_token_omits_auth_header(monkeypatch):
    """No DJ_API_TOKEN → no Authorization header (None passed to httpx)."""
    monkeypatch.delenv("DJ_API_TOKEN", raising=False)
    monkeypatch.setenv("DJ_MCP_URL", "http://example.com/mcp")

    captured: dict = {}

    class _FakeStreamCtx:
        async def __aenter__(self):
            return (MagicMock(), MagicMock(), lambda: None)

        async def __aexit__(self, *exc):
            return False

    def fake_streamablehttp_client(url, headers=None, timeout=None):
        captured["headers"] = headers
        return _FakeStreamCtx()

    class _FakeSessionCtx:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            session = MagicMock()
            session.initialize = AsyncMock()
            return session

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "streamablehttp_client", fake_streamablehttp_client)
    monkeypatch.setattr(cli, "ClientSession", _FakeSessionCtx)
    monkeypatch.setattr(cli, "_serve", AsyncMock())

    await cli.main()
    assert captured["headers"] is None


def test_run_invokes_asyncio_run(monkeypatch):
    """``run()`` is the script entrypoint — it should drive ``main()`` via asyncio."""

    invoked = {}

    def fake_asyncio_run(coro):
        invoked["called"] = True
        coro.close()  # avoid "coroutine was never awaited" warning

    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    cli.run()
    assert invoked["called"]


def test_run_swallows_keyboard_interrupt(monkeypatch):
    """``Ctrl-C`` in the CLI should log and exit cleanly, not propagate."""

    def fake_asyncio_run(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    cli.run()  # must not raise


def test_run_logs_and_exits_on_unhandled_exception(monkeypatch):
    """Any other exception is logged and the process exits with code 1."""

    def fake_asyncio_run(coro):
        coro.close()
        raise RuntimeError("boom")

    exits: list = []

    def fake_exit(code):
        exits.append(code)

    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(cli.sys, "exit", fake_exit)
    cli.run()
    assert exits == [1]


# ---------------------------------------------------------------------------
# Proxy handlers — verify each MCP method forwards to the upstream session
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_proxy_list_tools_forwards_to_upstream():
    """``list_tools`` returns the upstream's tools verbatim, normalised to list."""

    upstream = MagicMock()
    upstream.list_tools = AsyncMock(
        return_value=MagicMock(
            tools=(
                types.Tool(name="foo", description="d", inputSchema={"type": "object"}),
            ),
        ),
    )

    out = await cli._proxy_list_tools(upstream)
    assert [t.name for t in out] == ["foo"]
    upstream.list_tools.assert_awaited_once()


@pytest.mark.asyncio
async def test_proxy_call_tool_forwards_arguments():
    """``call_tool`` forwards (name, arguments) and returns the content list."""

    upstream = MagicMock()
    block = types.TextContent(type="text", text="ok")
    upstream.call_tool = AsyncMock(return_value=MagicMock(content=(block,)))

    out = await cli._proxy_call_tool(upstream, "foo", {"k": "v"})
    assert out == [block]
    upstream.call_tool.assert_awaited_once_with("foo", arguments={"k": "v"})


@pytest.mark.asyncio
async def test_proxy_call_tool_normalises_none_arguments():
    """If ``arguments`` is None it's coerced to an empty dict before forwarding."""

    upstream = MagicMock()
    upstream.call_tool = AsyncMock(return_value=MagicMock(content=()))

    out = await cli._proxy_call_tool(upstream, "foo", None)  # type: ignore[arg-type]
    assert out == []
    upstream.call_tool.assert_awaited_once_with("foo", arguments={})


@pytest.mark.asyncio
async def test_proxy_list_resources_forwards_to_upstream():
    """``list_resources`` returns the upstream's resources verbatim."""

    upstream = MagicMock()
    res = types.Resource(uri="dj://x", name="x")
    upstream.list_resources = AsyncMock(return_value=MagicMock(resources=(res,)))

    out = await cli._proxy_list_resources(upstream)
    assert out == [res]


@pytest.mark.asyncio
async def test_proxy_read_resource_returns_first_text_block():
    """``read_resource`` surfaces the first ``text`` block in the upstream response."""

    upstream = MagicMock()
    block_no_text = MagicMock(spec=[])  # no .text attribute
    block_with_text = MagicMock()
    block_with_text.text = "hello"
    upstream.read_resource = AsyncMock(
        return_value=MagicMock(contents=[block_no_text, block_with_text]),
    )

    out = await cli._proxy_read_resource(upstream, "dj://x")
    assert out == "hello"


@pytest.mark.asyncio
async def test_build_proxy_app_registers_all_four_handlers():
    """``_build_proxy_app`` returns a Server with handlers for every MCP method."""

    upstream = MagicMock()
    app = cli._build_proxy_app(upstream)
    assert isinstance(app, Server)
    # The MCP SDK stores registered handlers in ``request_handlers`` keyed by
    # the request type. We just need to confirm all four are wired.
    handler_count = len(app.request_handlers)
    assert handler_count >= 4


@pytest.mark.asyncio
async def test_serve_drives_app_run_with_stdio_streams(monkeypatch):
    """``_serve`` opens stdio_server, builds the proxy app, runs it. Stub
    everything so we just verify the wiring."""

    sentinel_app = MagicMock()
    sentinel_app.run = AsyncMock()
    sentinel_app.create_initialization_options = MagicMock(return_value="opts")

    monkeypatch.setattr(cli, "_build_proxy_app", lambda upstream: sentinel_app)

    class _StdioCtx:
        async def __aenter__(self):
            return ("read-stream", "write-stream")

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "stdio_server", lambda: _StdioCtx())

    await cli._serve(MagicMock())
    sentinel_app.run.assert_awaited_once_with(
        "read-stream",
        "write-stream",
        "opts",
    )
