"""
CLI entry point for DataJunction MCP Server
"""

import asyncio
import logging
import sys

from mcp.server.stdio import stdio_server

from datajunction.mcp.server import app
from datajunction.mcp.config import get_mcp_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,  # Log to stderr so stdout is clean for MCP protocol
)

logger = logging.getLogger(__name__)


async def main():
    """Main entry point for MCP server"""
    settings = get_mcp_settings()

    logger.info("Starting DataJunction MCP Server")
    logger.info(f"Connecting to DJ API at: {settings.dj_api_url}")

    # Run the MCP server using stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


def run():
    """Synchronous wrapper for async main"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("MCP Server stopped by user")
    except Exception as e:
        logger.error(f"MCP Server error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run()
