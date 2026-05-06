"""
Stdio→HTTP proxy for the DataJunction MCP server.

The server-side MCP implementation now lives in ``datajunction-server``
(`/mcp` endpoint, Streamable HTTP transport). This CLI exists so users on
clients that only speak stdio (e.g. Claude Desktop today) can still talk
to a hosted DJ MCP without running anything heavyweight locally.

It opens an HTTP MCP client to the configured hosted endpoint, hosts a
stdio MCP server, and forwards every list_tools / call_tool / list_resources /
read_resource request to the upstream. The wire shape is identical, so
clients see the same tool surface as the hosted server exposes.
"""

from __future__ import annotations

import asyncio
import logging
import sys

import mcp.types as types
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.server import Server
from mcp.server.stdio import stdio_server

from datajunction.mcp.config import get_mcp_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # stdout is reserved for MCP protocol traffic
)
logger = logging.getLogger(__name__)


async def _proxy_list_tools(upstream: ClientSession) -> list[types.Tool]:
    """Forward ``tools/list`` to the upstream session."""
    result = await upstream.list_tools()
    return list(result.tools)


async def _proxy_call_tool(
    upstream: ClientSession,
    name: str,
    arguments: dict,
) -> list[types.ContentBlock]:
    """Forward ``tools/call`` to the upstream session.

    ``result.content`` may be a tuple/list of content blocks; normalize to list.
    """
    result = await upstream.call_tool(name, arguments=arguments or {})
    return list(result.content)


async def _proxy_list_resources(upstream: ClientSession) -> list[types.Resource]:
    """Forward ``resources/list`` to the upstream session."""
    result = await upstream.list_resources()
    return list(result.resources)


async def _proxy_read_resource(upstream: ClientSession, uri: str) -> str:
    """Forward ``resources/read`` and surface the first text payload.

    The hosted DJ doesn't expose any resources today, but if it ever does
    we return the first ``text`` content block.
    """
    result = await upstream.read_resource(uri)  # type: ignore[arg-type]
    for block in result.contents:
        text = getattr(block, "text", None)
        if text is not None:
            return text
    return ""  # pragma: no cover


def _build_proxy_app(upstream: ClientSession) -> Server:
    """Create an MCP ``Server`` whose handlers all forward to ``upstream``.

    Split out from ``_serve`` so the wiring is testable without spinning
    up stdio.
    """
    app: Server = Server("datajunction")
    app.list_tools()(lambda: _proxy_list_tools(upstream))
    app.call_tool()(
        lambda name, arguments: _proxy_call_tool(upstream, name, arguments),
    )
    app.list_resources()(lambda: _proxy_list_resources(upstream))
    app.read_resource()(lambda uri: _proxy_read_resource(upstream, uri))
    return app


async def _serve(upstream: ClientSession) -> None:
    """Run a stdio MCP server that forwards every request to ``upstream``."""
    app = _build_proxy_app(upstream)
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


async def main() -> None:
    """Connect to the hosted DJ MCP and bridge it to stdio."""
    settings = get_mcp_settings()

    headers = {}
    if settings.dj_api_token:
        headers["Authorization"] = f"Bearer {settings.dj_api_token}"

    logger.info("Bridging stdio MCP → %s", settings.mcp_url)

    async with streamablehttp_client(
        url=settings.mcp_url,
        headers=headers or None,
        timeout=settings.request_timeout,
    ) as (read, write, _get_session_id):
        async with ClientSession(read, write) as upstream:
            await upstream.initialize()
            await _serve(upstream)


def run() -> None:
    """Synchronous entrypoint for ``dj-mcp`` script registration."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("dj-mcp stopped by user")
    except Exception as exc:
        logger.error("dj-mcp error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
