---
name: datajunction-query
description: |
  Activate this skill for querying DataJunction (DJ) — finding nodes,
  generating SQL, fetching metric data, exploring lineage, visualizing
  results — via the DJ UI, MCP tools, or REST/GraphQL APIs. For core DJ
  concepts and vocabulary, invoke `datajunction`. For modeling decisions
  (what shape something should take), invoke `datajunction-semantic-model`.
  For authoring nodes, invoke `datajunction-repo` (YAML) or
  `datajunction-api` (REST).
  Keywords:
  - query metric, query metrics
  - generate SQL, build metric SQL
  - get metric data, fetch metric
  - available dimensions, common dimensions
  - search_nodes, get_node_details, get_node_lineage
  - get_common_dimensions, build_metric_sql, get_metric_data
  - visualize metrics
  - MCP tools, DJ API, GraphQL
  - DJ UI, web UI, browse
user-invocable: false
---

# DataJunction Query

Consumer-side workflow for DJ: find existing nodes, build SQL to query them, fetch data, visualize results. For the underlying concepts (node types, dimension links, star schema), invoke the `datajunction` skill.

## DJ UI (Web)

For interactive exploration — browsing namespaces, inspecting a node's lineage / SQL / available dimensions, building queries by clicking dimensions on/off — the DJ web UI is usually the fastest path. It's hosted at the same URL as the DJ server (e.g. `https://your-dj-server.example.com/`).

**Use the UI when:**
- The user wants to *look around* — browse what exists, read a node's description, explore lineage visually
- They want to build a query interactively and copy the SQL out
- They're sharing a node or query with a teammate (UI URLs are shareable links)

**Use the MCP tools / API (below) when:**
- You need programmatic access — pulling node names into a script, generating SQL as part of a workflow, fetching data into a notebook
- The interaction is "give me this answer," not "let me explore"

Suggest opening the UI when the user's question is exploratory and you don't yet know the right node name. Suggest MCP tools when you have a name in hand and need data, lineage, or generated SQL.

## Discovery & Exploration (Use MCP Tools)

**When you need to explore the DJ semantic layer, use these MCP tools:**

### Find Available Nodes

**Use MCP tool**: `search_nodes`
- Search for metrics, dimensions, cubes by name or namespace
- Filter by node type
- Returns list of matching nodes

**Example**: `search_nodes(query="revenue", node_type="metric", namespace="finance")`

### Get Node Details

**Use MCP tool**: `get_node_details`
- Get comprehensive information about a specific node
- Returns: SQL definition, description, available dimensions, lineage, tags
- Input: Full node name (e.g., "finance.total_revenue")

**Example**: `get_node_details(name="finance.total_revenue")`

**What you get**:
- Description and SQL definition
- All available dimensions (via dimension links)
- Metric metadata (unit, direction, required dimensions)
- Upstream dependencies
- Tags and collections

### Check Common Dimensions

**Use MCP tool**: `get_common_dimensions`
- Find dimensions available across multiple metrics
- Essential before querying multiple metrics together
- Returns only dimensions shared by ALL specified metrics

**Example**: `get_common_dimensions(metric_names=["finance.total_revenue", "growth.daily_active_users"])`

**When to use**: Always check this before building queries with multiple metrics!

### Get Node Lineage

**Use MCP tool**: `get_node_lineage`
- Get upstream dependencies (what data sources this uses)
- Get downstream dependencies (what will break if you change this)
- Direction: "upstream", "downstream", or "both"

**Example**: `get_node_lineage(node_name="finance.total_revenue", direction="both")`

---

## Querying & SQL Generation (Use MCP Tools)

**When you need to query metrics or generate SQL, use these MCP tools:**

### Build Metric SQL

**Use MCP tool**: `build_metric_sql`
- Generate executable SQL for querying metrics
- Supports filters, dimensions, ordering, limits
- Returns SQL query and metadata

**Example**:
```
build_metric_sql(
  metrics=["finance.total_revenue"],
  dimensions=["core.date.date"],
  filters=["core.date.date >= '2024-01-01'"],
  orderby=["core.date.date ASC"],
  limit=100,
  dialect="trino",
  include_temporal_filters=True  # Enable automatic partition filtering (if cube has temporal partition)
)
```

**Returns**:
- Generated SQL for specified engine
- Output columns with types
- Cube name (if materialized cube used)

**Performance tip**: Use `include_temporal_filters=True` when querying cubes with temporal partitions to enable automatic partition filtering.

### Get Metric Data

**Use MCP tool**: `get_metric_data`
- Execute query and get actual data
- Returns query results as rows
- **IMPORTANT**: Only works with materialized cubes (refuses expensive ad-hoc queries)

**Example**:
```
get_metric_data(
  metrics=["finance.total_revenue", "finance.transaction_count"],
  dimensions=["core.date.date", "core.region.region_name"],
  filters=["core.date.date >= '2024-01-01'"],
  orderby=["core.date.date ASC"],
  limit=1000
)
```

**Best practices**:
- Always set a reasonable `limit`
- Use specific date range filters
- Check common dimensions first for multi-metric queries

### Visualize Metrics

**Use MCP tool**: `visualize_metrics`
- Query metrics and generate ASCII chart visualization
- Creates terminal-friendly charts (line, bar, scatter)
- **IMPORTANT**: Requires materialized cube

**Example**:
```
visualize_metrics(
  metrics=["finance.total_revenue"],
  dimensions=["core.date.date"],
  filters=["core.date.date >= '2024-01-01'"],
  orderby=["core.date.date ASC"],
  limit=90,
  chart_type="line"
)
```

**Chart types**:
- `line`: Time series (default)
- `bar`: Categorical comparisons
- `scatter`: Correlation analysis

---

## API Reference (Educational Context)

The MCP tools call the DJ REST and GraphQL APIs under the hood. Here's what they're doing:

### REST API Endpoints

**Node discovery**:
```bash
GET /nodes?node_type=metric&namespace=finance
GET /nodes/{node_name}
```

**SQL generation** (⚠️ Always use V3):
```bash
GET /sql/metrics/v3    # Generate query SQL
GET /sql/measures/v3   # Generate pre-aggregation SQL
```

**Dimension compatibility**:
```bash
GET /metrics/common/dimensions
```

### GraphQL API

Available at `/graphql`:

```graphql
query {
  nodes(nodeType: METRIC, namespace: "finance") {
    name
    description
    dimensions { name type }
  }

  commonDimensions(nodes: ["finance.revenue", "growth.users"]) {
    name
    type
  }
}
```
