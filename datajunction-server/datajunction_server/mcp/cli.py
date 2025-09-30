"""
Command-line interface for DataJunction MCP Server
"""

import asyncio
import sys
from pathlib import Path

# Add the parent directory to the Python path to ensure imports work
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datajunction_server.mcp.server_claude import main


def cli():
    """CLI entry point for dj-mcp command"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMCP server stopped by user")
    except Exception as e:
        print(f"Error running MCP server: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    cli()
