---
title: "Using DataJunction with AI Assistants"
draft: false
images: []
menu:
  docs:
    parent: ""
weight: 10
toc: true
---

DataJunction integrates with AI assistants like Claude, letting you explore your semantic layer, generate SQL, and query metrics through natural conversation — without writing code or crafting API requests directly.

The integration has three components that work together:

- **MCP tools** — give Claude the ability to query your live DJ instance: search nodes, generate SQL, inspect lineage, run queries, and more. Built on [MCP (Model Context Protocol)](https://modelcontextprotocol.io), an open standard for connecting AI assistants to external tools.
- **DJ skill** — teaches Claude about DJ concepts: node types, YAML syntax, dimension links, cube partitions, and semantic modeling best practices. Shapes how Claude reasons about DJ, not just what it can call.
- **DJ subagent** — a Claude Code agent with the DJ skill pre-loaded, so DJ expertise is automatically available in any Claude Code session without needing to invoke it manually.

## Installation

### Prerequisites

- Python 3.10 or higher
- Access to a running DataJunction server instance
- Claude Code (CLI) or Claude Desktop

### Install and set up

Install the DataJunction Python client with the MCP extra:

```bash
pip install datajunction[mcp]
```

Then run the setup command to configure Claude Code:

```bash
dj setup-claude
```

This installs all three components described above:
1. **DJ skill** — adds DataJunction knowledge to Claude Code under `~/.claude/skills/datajunction/`
2. **DJ subagent** — creates `~/.claude/agents/dj.md` so DJ expertise is always available
3. **MCP server config** — adds `dj-mcp` to `~/.claude.json` pointing at your DJ instance

Restart Claude Code after running to pick up the changes.

**Custom DJ server URL:**

```bash
DJ_URL=https://dj.yourcompany.com dj setup-claude
```

**Selective installation** (if you only want some components):

```bash
dj setup-claude --no-mcp         # Skill + subagent only
dj setup-claude --no-skills      # MCP + subagent only
dj setup-claude --no-agents      # Skill + MCP only
```

## Claude Desktop

`dj setup-claude` only configures Claude Code. For Claude Desktop, add the DJ MCP server manually to your config file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "dj-mcp",
      "env": {
        "DJ_API_URL": "http://localhost:8000",
        "DJ_USERNAME": "admin",
        "DJ_PASSWORD": "admin"
      }
    }
  }
}
```

To authenticate with a JWT token instead of username/password, use `DJ_API_TOKEN`:

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "dj-mcp",
      "env": {
        "DJ_API_URL": "https://dj.yourcompany.com",
        "DJ_API_TOKEN": "your-jwt-token-here"
      }
    }
  }
}
```

Restart Claude Desktop after saving.

## Available Tools

Once configured, the following tools are available to Claude:

### Discovery & Navigation

**`list_namespaces`**
List all available namespaces with node counts. Namespaces are the primary organizational structure in DataJunction (e.g., `finance.metrics`, `growth.dimensions`).

**`search_nodes`**
Search for nodes (metrics, dimensions, cubes, sources, transforms). All filters are optional and combinable. When searching git-backed namespaces, automatically resolves to main branches (e.g., `namespace="finance"` → `"finance.main"`).

Parameters:
- `query` (optional): Fragment of node name to search for (e.g., `revenue`)
- `node_type` (optional): Filter by type — `metric`, `dimension`, `cube`, `source`, `transform`
- `namespace` (optional): Filter by namespace (highly recommended to narrow results)
- `tags` (optional): Filter to nodes tagged with ALL of these tag names (e.g., `["revenue", "core"]`)
- `statuses` (optional): Filter by validity — `["valid"]` for healthy nodes, `["invalid"]` to find broken ones
- `mode` (optional): Filter by `published` (production) or `draft` (in-progress work on a branch)
- `owned_by` (optional): Filter to nodes owned by this username or email
- `has_materialization` (optional): If `true`, return only nodes with materializations configured (default: `false`)
- `limit` (optional): Maximum results (default: 100, max: 1000)
- `prefer_main_branch` (optional): Auto-resolve to `.main` branches (default: `true`)

**`get_node_details`**
Get detailed information about a specific node including its SQL definition, metadata, tags, owners, and dependencies.

Parameters:
- `name` (required): Full node name (e.g., `finance.daily_revenue`)

### Lineage & Dependencies

**`get_node_lineage`**
Explore upstream dependencies (what this node depends on) and downstream dependencies (what depends on this node). Useful for impact analysis and understanding data flow.

Parameters:
- `node_name` (required): Full node name
- `direction` (optional): `upstream`, `downstream`, or `both` (default: `both`)
- `max_depth` (optional): Maximum traversal depth

**`get_node_dimensions`**
List all dimensions available for a specific node, showing which dimensions can be used for grouping/filtering.

Parameters:
- `node_name` (required): Full node name

### Analysis & Querying

**`get_common`**
Bidirectional semantic compatibility lookup. Provide exactly one of `metrics` or `dimensions`:

- Pass `metrics` → returns the dimensions shared across all of those metrics (i.e., what can I slice these metrics by?)
- Pass `dimensions` → returns the metrics that can be queried using all of those dimensions (i.e., what can I analyze by this dimension?)

Parameters:
- `metrics` (optional): List of metric node names
- `dimensions` (optional): List of dimension attribute names

**`get_query_plan`**
Get the query execution plan for a set of metrics, showing how DataJunction decomposes them internally. The plan includes:

- **Grain groups** — sets of metrics that share a common dimensional grain and can be computed in a single SQL query
- **Components** — the atomic aggregations (e.g., `SUM(amount)`, `COUNT(*)`) that feed into each metric
- **Metric formulas** — the combiner expressions that reassemble components into final metric values

Use this to understand multi-metric query structure, debug unexpected results, or validate your semantic model design.

Parameters:
- `metrics` (required): List of metric names to analyze
- `dimensions` (optional): Dimensions to group by — affects grain group assignment
- `filters` (optional): SQL filter conditions
- `dialect` (optional): Target SQL dialect (e.g., `spark`, `trino`, `postgres`)
- `use_materialized` (optional): Use materialized tables when available (default: `true`)
- `include_temporal_filters` (optional): Include partition filters if metrics resolve to a cube with partitions (default: `false`)
- `lookback_window` (optional): Lookback window for temporal filters when `include_temporal_filters` is `true` (e.g., `7 DAY`, `1 WEEK`)

**`build_metric_sql`**
Generate executable SQL for querying metrics with specified dimensions and filters. Returns the SQL query, output columns, and dialect.

Parameters:
- `metrics` (required): List of metric names
- `dimensions` (optional): List of dimensions to group by
- `filters` (optional): SQL filter conditions
- `orderby` (optional): Columns to order by (use full node names, e.g., `finance.revenue DESC`)
- `limit` (optional): Row limit
- `dialect` (optional): Target SQL dialect

**`get_metric_data`**
Execute a query and return actual data results. Only works with materialized cubes — refuses to run expensive ad-hoc queries.

Parameters:
- `metrics` (required): List of metric names
- `dimensions` (optional): List of dimensions to group by
- `filters` (optional): SQL filter conditions
- `orderby` (optional): Columns to order by
- `limit` (optional): Row limit (recommended)

## Usage Examples

Once configured, you can ask Claude questions like:

- "What namespaces are available in DataJunction?"
- "Show me all published revenue metrics in the finance namespace"
- "Which metrics have a materialization configured?"
- "Find all invalid nodes in the growth namespace"
- "What dimensions do revenue and cost metrics have in common?"
- "Which metrics can I slice by `common.dimensions.date.dateint`?"
- "Show me the query plan for `finance.revenue` and `finance.orders` together"
- "Generate SQL to query daily revenue grouped by region"
- "What nodes depend on the users dimension?"
- "Show me actual revenue data for the last 7 days by region"

Claude will automatically use the appropriate tools to answer your questions.

## Git-Backed Namespaces

Many DataJunction deployments use git branches to separate development and production nodes. Namespaces follow a pattern like:

- `finance.main` - Production metrics
- `finance.feature1` - Development/experimental metrics

When you search with `namespace="finance"`, the MCP server automatically resolves to `finance.main` (if it exists) to ensure you get production-ready nodes. Set `prefer_main_branch=False` to search all branches.

Search results show git branch information: `[git: company/finance-metrics @ main]`

## Testing the Installation

Test your setup in Claude:

1. Open Claude Desktop or start Claude Code
2. Start a new conversation
3. Ask: "What namespaces are available in DataJunction?"
4. Claude should use the `list_namespaces` tool to query your DJ server

If successful, you'll see a list of namespaces with node counts.

## Troubleshooting

### MCP Server Not Found

If you get a "command not found" error:

1. **Check installation**: Run `which dj-mcp` to verify it's in your PATH
2. **Use full path**: Specify the absolute path to `dj-mcp` in the Claude config
3. **Virtual environment**: If using a venv, use the full path to the venv's bin directory

### Authentication Errors

If you get authentication errors:

1. **Verify credentials**: Test them with curl:
   ```bash
   curl -X POST http://localhost:8000/basic/login/ \
     -d "username=admin&password=admin" \
     -H "Content-Type: application/x-www-form-urlencoded"
   ```

2. **Check API URL**: Ensure `DJ_API_URL` points to your running DataJunction server
3. **Check logs**: Claude Code logs are in `~/.claude/debug/latest`

### Connection Refused

If the MCP server can't connect to DataJunction:

1. **Verify DJ is running**: Check that your DataJunction server is accessible
2. **Check URL**: Ensure `DJ_API_URL` is correct (including http:// or https://)
3. **Network access**: Verify there are no firewall rules blocking the connection

### GraphQL Errors

If you see GraphQL errors in the response:

1. **Check DJ version**: Ensure your DJ server is up to date
2. **Verify schema**: The MCP server expects the latest GraphQL schema
3. **Check server logs**: Look at DJ server logs for more details

### Debugging

Enable debug logging by checking Claude Code's debug logs:

```bash
tail -f ~/.claude/debug/latest
```

The MCP server also writes its own debug log to `~/.dj_mcp_debug.log`.

## Architecture

The MCP server is built on:

1. **Server Core** (`datajunction/mcp/server.py`): MCP protocol implementation using the official Python SDK
2. **Tools** (`datajunction/mcp/tools.py`): Business logic for each tool, communicating with DJ's GraphQL API
3. **Formatters** (`datajunction/mcp/formatters.py`): Converts GraphQL responses to AI-friendly text
4. **CLI** (`datajunction/mcp/cli.py`): Command-line interface for starting the server

The server runs as a separate process from the DJ API server, communicating via stdin/stdout with Claude and via GraphQL with DataJunction.

## Uninstalling

To remove the DataJunction MCP server:

```bash
pip uninstall datajunction
```

Then remove the `datajunction` entry from your Claude configuration file.

## Support

- **Documentation**: [DataJunction Docs](https://datajunction.io)
- **GitHub Issues**: [Report issues](https://github.com/DataJunction/dj/issues)
- **Source Code**: [GitHub Repository](https://github.com/DataJunction/dj)
