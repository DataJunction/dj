---
title: "Using DataJunction with AI Assistants (MCP)"
draft: false
images: []
menu:
  docs:
    parent: ""
weight: 10
toc: true
---

The DataJunction MCP (Model Context Protocol) server allows AI assistants like Claude to interact directly with your DataJunction semantic layer. Instead of writing code or crafting API queries, you can have natural conversations with AI to discover metrics, explore relationships, generate SQL, and query data.

## What is MCP?

The Model Context Protocol (MCP) is an open-source standard created by Anthropic for connecting AI assistants to external systems. MCP allows Claude to discover available tools, call them with appropriate parameters, and use the results to help you work with DataJunction.

## Installation

### Prerequisites

- Python 3.10 or higher
- Access to a running DataJunction server instance
- Claude Desktop or Claude Code (CLI)

### Install from PyPI

```bash
pip install datajunction[mcp]
```

### Install from GitHub

Install the latest version directly from GitHub:

```bash
pip install git+https://github.com/DataJunction/dj.git#subdirectory=datajunction-clients/python
```

Install a specific branch:

```bash
pip install git+https://github.com/DataJunction/dj.git@branch-name#subdirectory=datajunction-clients/python
```

### Install from Source

If you've cloned the repository:

```bash
cd datajunction-clients/python
uv pip install -e .
```

Or for development:

```bash
uv install
```

### Verify Installation

Check that the MCP server is installed correctly:

```bash
dj-mcp --help
```

The server will start and wait for stdin/stdout communication (this is normal - it communicates via pipes, not HTTP).

## Configuration

{{< alert icon="💡" >}}
**You don't need to manually run `dj-mcp`** - Claude automatically starts and stops it as needed based on your configuration.
{{< /alert >}}

### Claude Desktop

The Claude Desktop configuration file is located at:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

Edit the configuration file and add the DataJunction MCP server:

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "dj-mcp",
      "args": [],
      "env": {
        "DJ_API_URL": "http://localhost:8000",
        "DJ_USERNAME": "admin",
        "DJ_PASSWORD": "admin"
      }
    }
  }
}
```

After saving, restart Claude Desktop to load the MCP server.

### Claude Code (CLI)

For Claude Code, add the configuration to `~/.claude/mcp_settings.json`:

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "dj-mcp",
      "args": [],
      "env": {
        "DJ_API_URL": "http://localhost:8000",
        "DJ_USERNAME": "admin",
        "DJ_PASSWORD": "admin"
      }
    }
  }
}
```

**Alternative:** You can also use a project-specific configuration by creating `.mcp.json` in your project directory:

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "dj-mcp",
      "args": [],
      "env": {
        "DJ_API_URL": "http://localhost:8000",
        "DJ_USERNAME": "admin",
        "DJ_PASSWORD": "admin"
      }
    }
  }
}
```

### Configuration Options

The MCP server supports the following environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DJ_API_URL` | URL of your DataJunction server | `http://localhost:8000` | Yes |
| `DJ_API_TOKEN` | JWT token for authentication | - | No* |
| `DJ_USERNAME` | Username for basic auth | - | No* |
| `DJ_PASSWORD` | Password for basic auth | - | No* |

\* Either provide `DJ_API_TOKEN` OR both `DJ_USERNAME` and `DJ_PASSWORD`

### Configuration Examples

**Local Development:**

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

**Production with JWT Token:**

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

**Using a Virtual Environment:**

If you installed the MCP server in a virtual environment, specify the full path:

```json
{
  "mcpServers": {
    "datajunction": {
      "command": "/path/to/venv/bin/dj-mcp",
      "env": {
        "DJ_API_URL": "http://localhost:8000",
        "DJ_USERNAME": "admin",
        "DJ_PASSWORD": "admin"
      }
    }
  }
}
```

## Available Tools

Once configured, the following tools are available to Claude:

### Discovery & Navigation

**`list_namespaces`**
List all available namespaces with node counts. Namespaces are the primary organizational structure in DataJunction (e.g., `finance.metrics`, `growth.dimensions`).

**`search_nodes`**
Search for nodes (metrics, dimensions, cubes, sources, transforms) by name fragment. Supports filtering by type and namespace. When searching git-backed namespaces, automatically resolves to main branches (e.g., `namespace="finance"` → `"finance.main"`).

Parameters:
- `query` (required): Search term
- `node_type` (optional): Filter by type (metric, dimension, cube, source, transform)
- `namespace` (optional): Filter by namespace (highly recommended)
- `limit` (optional): Maximum results (default: 100, max: 1000)
- `prefer_main_branch` (optional): Auto-resolve to .main branches (default: true)

**`get_node_details`**
Get detailed information about a specific node including its SQL definition, metadata, tags, owners, and dependencies.

Parameters:
- `name` (required): Full node name (e.g., `finance.daily_revenue`)

### Lineage & Dependencies

**`get_node_lineage`**
Explore upstream dependencies (what this node depends on) and downstream dependencies (what depends on this node). Useful for impact analysis and understanding data flow.

Parameters:
- `node_name` (required): Full node name
- `direction` (optional): "upstream", "downstream", or "both" (default: "both")
- `max_depth` (optional): Maximum traversal depth

**`get_node_dimensions`**
List all dimensions available for a specific node, showing which dimensions can be used for grouping/filtering.

Parameters:
- `node_name` (required): Full node name

### Analysis & Querying

**`get_common_dimensions`**
Find dimensions that work across multiple metrics. Essential for determining whether metrics can be queried together.

Parameters:
- `metric_names` (required): List of metric names to analyze

**`build_metric_sql`**
Generate executable SQL for querying metrics with specified dimensions and filters. Returns the SQL query, output columns, and dialect.

Parameters:
- `metrics` (required): List of metric names
- `dimensions` (optional): List of dimensions to group by
- `filters` (optional): SQL filter conditions
- `orderby` (optional): Columns to order by
- `limit` (optional): Row limit
- `dialect` (optional): Target SQL dialect

**`get_metric_data`**
Execute a query and return actual data results. Use this when you want to see data values, not just SQL.

Parameters:
- `metrics` (required): List of metric names
- `dimensions` (optional): List of dimensions to group by
- `filters` (optional): SQL filter conditions
- `orderby` (optional): Columns to order by
- `limit` (optional): Row limit (recommended to avoid large result sets)
- `use_materialized` (optional): Whether to use materialized tables (default: true)

## Usage Examples

Once configured, you can ask Claude questions like:

- "What namespaces are available in DataJunction?"
- "Show me revenue metrics in the finance namespace"
- "What dimensions do revenue and cost metrics have in common?"
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

This shows all MCP communication and API requests.

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
