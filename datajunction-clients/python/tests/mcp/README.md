# dj-mcp CLI tests

The DataJunction MCP server is hosted in `datajunction-server` at the
`/mcp` endpoint (Streamable HTTP transport). The client package only
ships a thin stdio→HTTP proxy CLI (`dj-mcp`) for stdio-only MCP clients
like Claude Desktop.

These tests cover the proxy wiring:

- env vars (`DJ_API_TOKEN`, `DJ_MCP_URL`) → upstream URL + Authorization header
- `run()` invokes `asyncio.run(main())`

End-to-end tool behavior is covered server-side under
`datajunction-server/tests/dj_mcp/`. There's no per-tool coverage here
because the CLI doesn't implement tools — it forwards every request to
the hosted server.
