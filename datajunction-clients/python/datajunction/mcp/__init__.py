"""
DataJunction MCP stdio→HTTP proxy.

The canonical MCP server is hosted by ``datajunction-server`` at the
``/mcp`` endpoint (Streamable HTTP transport). This package provides a
small ``dj-mcp`` CLI that bridges stdio MCP clients (e.g. Claude Desktop)
to the hosted server, so users on stdio-only clients can still talk to a
deployed DJ.

For HTTP-capable clients (Claude Code, Cursor, Slack agents) connect to
the hosted ``/mcp`` endpoint directly — no local CLI required.
"""
