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


async def _serve(upstream: ClientSession) -> None:
    """Run a stdio MCP server that forwards every request to ``upstream``."""
    app: Server = Server("datajunction")

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        result = await upstream.list_tools()
        return list(result.tools)

    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.ContentBlock]:
        result = await upstream.call_tool(name, arguments=arguments or {})
        # ``result.content`` may be a tuple/list of content blocks; normalise.
        return list(result.content)

    @app.list_resources()
    async def list_resources() -> list[types.Resource]:
        result = await upstream.list_resources()
        return list(result.resources)

    @app.read_resource()
    async def read_resource(uri: str) -> str:
        result = await upstream.read_resource(uri)  # type: ignore[arg-type]
        # The hosted DJ doesn't expose any resources today, but if it ever
        # does, surface the first text/blob payload.
        for block in result.contents:
            text = getattr(block, "text", None)
            if text is not None:
                return text
        return ""  # pragma: no cover

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
