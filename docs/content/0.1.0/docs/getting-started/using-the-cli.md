---
title: "Using the DJ CLI"
draft: false
images: []
menu:
  docs:
    parent: "getting-started"
weight: 7
toc: true
---

The DataJunction CLI (`dj`) provides a command-line interface for managing your DataJunction deployment, nodes, and namespaces.

## Installation

The CLI is installed automatically with the DataJunction Python client:

```bash
pip install datajunction
```

## Configuration

### Authentication

Set your DJ credentials using environment variables:

```bash
export DJ_URL="http://localhost:8000"
export DJ_USER="your-username"
export DJ_PWD="your-password"
```

## Available Commands

### Node Inspection

#### `dj describe <node-name>`

Display detailed information about a node including its type, description, query, columns, and metadata.

**Usage:**
```bash
# Describe a node in text format
dj describe default.num_repair_orders

# Output as JSON
dj describe default.num_repair_orders --format json
```

**Options:**
- `--format <text|json>`: Output format (default: text)

**Example output:**
```
============================================================
Node: default.num_repair_orders
============================================================
Type:         metric
Description:  Number of repair orders
Status:       valid
Mode:         published

Query:
------------------------------------------------------------
SELECT COUNT(repair_order_id) FROM default.repair_orders
============================================================
```

#### `dj lineage <node-name>`

Show lineage (upstream and downstream dependencies) for a node.

**Usage:**
```bash
# Show both upstream and downstream dependencies
dj lineage default.num_repair_orders

# Show only upstream dependencies
dj lineage default.num_repair_orders --direction upstream

# Show only downstream dependencies
dj lineage default.num_repair_orders --direction downstream

# Output as JSON
dj lineage default.num_repair_orders --format json
```

**Options:**
- `--direction <upstream|downstream|both>`: Direction of lineage to show (default: both)
- `--format <text|json>`: Output format (default: text)

#### `dj dimensions <node-name>`

List all available dimensions for a node (particularly useful for metrics).

**Usage:**
```bash
# Show available dimensions for a metric
dj dimensions default.num_repair_orders

# Output as JSON
dj dimensions default.num_repair_orders --format json
```

**Options:**
- `--format <text|json>`: Output format (default: text)

**Example output:**
```
============================================================
Available dimensions for: default.num_repair_orders
============================================================

  • default.hard_hat.city
    Type: string
    Node: default.hard_hat
    Path: default.repair_orders → default.hard_hat

  • default.hard_hat.state
    Type: string
    Node: default.hard_hat
    Path: default.repair_orders → default.hard_hat

  • default.dispatcher.company_name
    Type: string
    Node: default.dispatcher
    Path: default.repair_orders → default.dispatcher

Total: 15 dimensions

============================================================
```

**Use case:** When you want to query a metric with specific dimensions, use this command to see all available dimension attributes you can group by or filter on.

### Listing Objects

#### `dj list <type>`

List various types of objects in DJ (namespaces, metrics, dimensions, cubes, sources, transforms, nodes).

**Usage:**
```bash
# List all metrics
dj list metrics

# List metrics in a specific namespace
dj list metrics --namespace default

# List all namespaces
dj list namespaces

# Output as JSON
dj list metrics --format json
```

**Supported types:**
- `namespaces`: List all namespaces
- `metrics`: List metric nodes
- `dimensions`: List dimension nodes
- `cubes`: List cube nodes
- `sources`: List source nodes
- `transforms`: List transform nodes
- `nodes`: List all nodes

**Options:**
- `--namespace <name>`: Filter by namespace (or prefix for namespaces)
- `--format <text|json>`: Output format (default: text)

### SQL Generation

#### `dj sql <node-name>`

Generate SQL for a node with optional dimensions and filters.

**Usage:**
```bash
# Generate SQL for a metric
dj sql default.num_repair_orders

# Generate SQL with dimensions
dj sql default.num_repair_orders --dimensions default.hard_hat.city,default.hard_hat.state

# Generate SQL with filters
dj sql default.num_repair_orders \
  --dimensions default.hard_hat.city \
  --filters "default.hard_hat.state = 'CA'"
```

**Options:**
- `--dimensions <dim1,dim2,...>`: Comma-separated list of dimensions
- `--filters <filter1,filter2,...>`: Comma-separated list of filters
- `--engine <name>`: Engine name
- `--engine-version <version>`: Engine version

### Data Retrieval

#### `dj data`

Fetch actual query results for a node or metrics with optional dimensions and filters.

**Usage:**
```bash
# Fetch data for a single metric node
dj data default.num_repair_orders

# Fetch data for multiple metrics
dj data --metrics default.num_repair_orders default.avg_repair_price

# Fetch data with dimensions
dj data --metrics default.avg_repair_price --dimensions default.hard_hat.city

# Fetch data with filters
dj data --metrics default.avg_repair_price \
  --dimensions default.hard_hat.city \
  --filters "default.hard_hat.state = 'CA'"

# Limit the number of rows returned
dj data --metrics default.avg_repair_price --limit 100

# Output in different formats
dj data --metrics default.avg_repair_price --format json
dj data --metrics default.avg_repair_price --format csv
```

**Options:**
- `--metrics <metric1> <metric2> ...`: One or more metrics to query
- `--dimensions <dim1> <dim2> ...`: Dimensions to group by
- `--filters <filter1> <filter2> ...`: Filter expressions
- `--limit <number>`: Maximum rows to return (default: 1000)
- `--format <table|json|csv>`: Output format (default: table)
- `--engine <name>`: Engine name
- `--engine-version <version>`: Engine version

**Example output (table format):**
```
╭──────────────────────────┬───────────────────╮
│ default.hard_hat.city    │ avg_repair_price  │
├──────────────────────────┼───────────────────┤
│ Jersey City              │ 54751.10          │
│ Billerica                │ 38277.67          │
│ Southgate                │ 33625.85          │
╰──────────────────────────┴───────────────────╯

3 row(s)
```

{{< alert icon="💡" text="Queries default to a limit of 1000 rows to prevent accidentally running unbounded queries on large tables. Use <code>--limit</code> to adjust this." />}}

### Query Planning

#### `dj plan`

Show the query execution plan for a set of metrics, including how metrics will be computed and which tables/cubes will be used.

**Usage:**
```bash
# Show execution plan for metrics
dj plan --metrics default.num_repair_orders default.avg_repair_price

# Show plan with dimensions
dj plan --metrics default.avg_repair_price --dimensions default.hard_hat.city

# Show plan with filters
dj plan --metrics default.avg_repair_price \
  --filters "default.hard_hat.state = 'CA'"

# Specify target dialect
dj plan --metrics default.avg_repair_price --dialect spark

# Output as JSON
dj plan --metrics default.avg_repair_price --format json
```

**Options:**
- `--metrics <metric1> <metric2> ...`: Metrics to include in the plan
- `--dimensions <dim1> <dim2> ...`: Dimensions to group by
- `--filters <filter1> <filter2> ...`: Filter expressions
- `--dialect <spark|trino|druid>`: Target SQL dialect
- `--format <text|json>`: Output format (default: text)

**Example output:**
```
============================================================
Query Execution Plan
============================================================
Dialect: spark
Requested Dimensions: default.hard_hat.city

Grain Groups (1)
------------------------------------------------------------
Group 1: default.repair_order_details
  Grain: [default.hard_hat.city]
  Aggregability: full
  Metrics: default.avg_repair_price

  Components:
    • price_count: COUNT(price)
    • price_sum: SUM(price)

Metric Formulas (1)
------------------------------------------------------------
  • default.avg_repair_price = SUM(price_sum) / SUM(price_count)

============================================================
```

**Use case:** Use this command to understand how DJ will execute a query before running it. This is helpful for debugging performance issues or understanding which materialized cubes will be used.

### Deployments

#### `dj push <directory>`

Push node YAML definitions from a local directory to the DJ server. Supports dry-run mode for impact analysis before deploying.

**Usage:**
```bash
# Push all YAML files in a directory
dj push ./my-nodes

# Override namespace in YAML files
dj push ./my-nodes --namespace my.custom.namespace

# Dry run to preview changes without applying them
dj push ./my-nodes --dryrun

# Dry run with JSON output for programmatic use
dj push ./my-nodes --dryrun --format json
```

**Options:**
- `--namespace <name>`: Override the namespace specified in YAML files
- `--dryrun`: Preview changes without applying them (shows impact analysis)
- `--format <text|json>`: Output format for dry run (default: text)
- `--repo <url>`: Git repository URL for deployment tracking
- `--branch <name>`: Git branch name for deployment tracking
- `--commit <sha>`: Git commit SHA for deployment tracking
- `--ci-system <name>`: CI system name (e.g., 'github_actions', 'jenkins')
- `--ci-run-url <url>`: URL to the CI run/build

**Example:**
```bash
dj push ./metrics --namespace production.analytics
```

**Example dry run output:**
```
Analyzing deployment from: ./my-nodes

Impact Analysis
============================================================

Changes to Apply:
------------------------------------------------------------
  CREATE  production.metrics.new_metric (metric)
  UPDATE  production.metrics.existing_metric (metric)

Downstream Impact:
------------------------------------------------------------
  production.metrics.existing_metric
    ↳ Downstream nodes affected: 3
      • production.dashboards.sales_cube
      • production.reports.daily_summary
      • production.alerts.threshold_metric

============================================================
```

{{< alert icon="💡" text="Always run <code>dj push --dryrun</code> first to preview changes before deploying to production namespaces." />}}

#### `dj pull <namespace> <directory>`

Pull (export) nodes from a namespace to YAML files in a local directory.

**Usage:**
```bash
# Export all nodes from default namespace
dj pull default ./exported-nodes

# Export from a specific namespace
dj pull production.metrics ./metrics-backup
```

**Example:**
```bash
# Backup all nodes in production namespace
dj pull production ./backups/production-$(date +%Y%m%d)
```

### Node Management

#### `dj delete-node <node-name>`

Delete (deactivate) or permanently remove a node.

**Usage:**
```bash
# Soft delete (deactivate) a node
dj delete-node default.old_metric

# Hard delete (permanently remove) a node
dj delete-node default.old_metric --hard
```

**Options:**
- `--hard`: Permanently delete the node (use with extreme caution)

**Example:**
```bash
# Deactivate a deprecated metric
dj delete-node default.deprecated_metric

# Permanently remove a test node
dj delete-node staging.test_metric --hard
```

{{< alert icon="⚠️" text="Soft delete (deactivate) is reversible via the Python API using <code>djbuilder.restore_node()</code>. Hard delete is permanent and cannot be undone." />}}

### Namespace Management

#### `dj delete-namespace <namespace>`

Delete (deactivate) or permanently remove a namespace.

**Usage:**
```bash
# Soft delete (deactivate) a namespace
dj delete-namespace old_project

# Delete namespace and all contained nodes
dj delete-namespace old_project --cascade

# Hard delete (permanently remove)
dj delete-namespace old_project --hard --cascade
```

**Options:**
- `--cascade`: Delete all nodes within the namespace
- `--hard`: Permanently delete the namespace (use with extreme caution)

**Example:**
```bash
# Remove a test environment namespace
dj delete-namespace test.environment --cascade

# Permanently clean up an old project
dj delete-namespace archived.old_project --hard --cascade
```

{{< alert icon="⚠️" text="Using <code>--cascade</code> with <code>--hard</code> will permanently delete the namespace and all its nodes. This cannot be undone." />}}

## Troubleshooting

### Authentication Issues

If you see authentication errors:

```bash
# Verify environment variables are set
echo $DJ_USER
echo $DJ_PWD

# Re-export with correct values
export DJ_USER="your-username"
export DJ_PWD="your-password"
```

### Connection Issues

If CLI cannot connect to DJ server:

```bash
# Check server URL
echo $DJ_URL

# Verify server is running
curl $DJ_URL/health

# Update URL if needed
export DJ_URL="http://your-dj-server:8000"
```

## Getting Help

```bash
# Show all available commands
dj --help

# Show help for specific command
dj push --help
dj delete-node --help
```
