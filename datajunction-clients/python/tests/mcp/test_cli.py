"""
Tests for the dj-mcp stdio→HTTP proxy CLI.

The CLI delegates everything to a hosted DJ MCP server via Streamable HTTP.
We just verify the wiring: settings → headers, upstream connect, stdio
bridge runs. Actual tool behavior is tested in datajunction-server's
``tests/dj_mcp/`` suite.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_main_connects_with_bearer_token_when_set(monkeypatch):
    """When DJ_API_TOKEN is set, it's forwarded as an Authorization header."""
    monkeypatch.setenv("DJ_API_TOKEN", "test-token")
    monkeypatch.setenv("DJ_MCP_URL", "http://example.com/mcp")

    from datajunction.mcp import cli

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

    from datajunction.mcp import cli

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
    from datajunction.mcp import cli

    invoked = {}

    def fake_asyncio_run(coro):
        invoked["called"] = True
        coro.close()  # avoid "coroutine was never awaited" warning

    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    cli.run()
    assert invoked["called"]
