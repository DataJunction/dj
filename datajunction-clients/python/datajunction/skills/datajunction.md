---
name: datajunction
description: |
  Activate this skill whenever working with DataJunction (DJ) semantic layer.
  Comprehensive guide covering core concepts, querying, building, and repo-backed workflows.
  Keywords:
  - DataJunction, DJ
  - semantic layer
  - dimension link, dimension links
  - star schema
  - metric, metrics
  - SQL generation
  - node types
  - create metric, create dimension, create node
  - query metric, query metrics
  - generate SQL
  - available dimensions, common dimensions
  - build cube, create cube
  - YAML nodes, YAML definitions
  - git workflow, repo-backed namespace
  - branch development, feature branch
user-invocable: false
---

# DataJunction

## What is DataJunction?

DataJunction is a semantic layer that provides a unified interface to query metrics across data sources using dimensional modeling (star schema).

**Key capabilities:**
- Define metrics once, query anywhere
- Automatic join path discovery via dimension links
- Multi-engine SQL generation (Trino, Spark, Druid, DuckDB, etc.)
- Lineage tracking and impact analysis
- Materialization for performance optimization

---

## Node Types

DataJunction organizes entities into different node types:

| Node Type | Description | Example |
|-----------|-------------|---------|
| **Source** | Physical table or view in your data warehouse | `catalog.finance.transactions_table` |
| **Transform** | SQL transformation of other nodes (cleaning, filtering, joining) | `events.clean.user_events` |
| **Dimension** | Entity with attributes (users, products, dates, regions) | `core.users`, `core.dates` |
| **Metric** | Aggregated measure (revenue, count, ratios) | `finance.total_revenue` |
| **Cube** | Pre-computed metric combinations for performance | `finance.revenue_cube` |

---

## Star Schema & Dimensional Modeling

DJ uses normalized star schema modeling where **dimension links** connect fact tables to dimension tables, and dimensional entities to other dimensions:

```
[Fact Table: transactions]
    ├─ dimension_link: user_id → [Dimension: users]
    │   ├─ Attributes: country, signup_date, tier
    │   │  └─ dimension_link: signup_date → [Dimension: date]
    │   │      └─ Attributes: month, quarter, year
    │   └─ Attributes: country, signup_date, tier
    ├─ dimension_link: product_id → [Dimension: products]
    │   └─ Attributes: category, price, brand
    └─ dimension_link: date → [Dimension: dates]
        └─ Attributes: month, quarter, year

[Metric: revenue]
    ├─ Defined on: transactions
    └─ Auto-inherits dimension links from transactions
        ├─ users.country (via user_id)
        ├─ products.category (via product_id)
        └─ dates.month (via date)
```

**Key insight**: When you define dimension links on a node, any metric built on that node automatically inherits access to all dimension attributes. DJ automatically discovers and generates the necessary joins across the entire dimensions graph.

---

## Dimension Links

Dimension links define how nodes join to dimensions. They enable automatic join path discovery.

**Basic structure:**
```yaml
dimension_links:
  - dimension: common.dimensions.users
    join_type: left              # Optional: left, right, inner (default: left)
    join_on: finance.transactions.user_id = common.dimensions.users.user_id
```

**Join types:**
- `left` - Left outer join (default, most common)
- `right` - Right outer join
- `inner` - Inner join (only matching rows)

### Where to Define Them

Dimension links can be defined on **three node types**:

#### 1. Source Nodes
When the source table cleanly represents a semantic entity.

#### 2. Transform Nodes (Clean Semantic Representation)
When source tables don't accurately represent your semantic entity - use transforms to clean data first.

#### 3. Dimension Nodes (Build Dimensional Graph)
Dimensions can link to other dimensions, creating a rich dimensional graph.

**Example dimensional graph:**
```
transactions → users → countries → regions
```

Metrics on `transactions` can now group by:
- `users.country_code` (1 hop)
- `countries.country_name` (2 hops via users → countries)
- `regions.population` (3 hops via users → countries → regions)

---

## Dimension Links with Roles

When a node references the same dimension node multiple times, use **roles** to disambiguate:

```json
{
  "dimension_links": [
    {
      "dimension": "core.users",
      "join_on": "orders.buyer_id = core.users.user_id",
      "role": "buyer"
    },
    {
      "dimension": "core.users",
      "join_on": "orders.seller_id = core.users.user_id",
      "role": "seller"
    }
  ]
}
```

**Query with**: `buyer.country` vs `seller.country`

DJ uses the role prefix to generate the correct joins.

---

## Node Status & Validity

DJ tracks TWO separate state systems:

### 1. Mode (User-Controlled Lifecycle)

Users control this via the `mode` field:

- **`draft`**: Work in progress
  - Use for development/testing
  - Can be invalid SQL - that's okay while drafting

- **`published`**: Production-ready
  - Should be valid and tested
  - Indicates that it's ready for use

### 2. Status (System-Controlled Validation)

DJ automatically validates nodes and sets status:

- **`valid`**: Passes all validation checks
  - SQL is syntactically correct
  - Referenced nodes exist
  - Metadata is correct
  - Dimension links are valid

- **`invalid`**: Failed validation
  - SQL syntax error
  - References non-existent node
  - Circular dependency
  - Dimension link broken

**Key insight**: `mode` and `status` are independent:

| Mode | Status | Meaning |
|------|--------|---------|
| `draft` | `valid` | Good draft, not yet published |
| `draft` | `invalid` | Work in progress, has errors |
| `published` | `valid` | Production-ready ✅ |
| `published` | `invalid` | Was working, now broken (upstream change) ⚠️ |

---

## Practical Notes & Constraints

### SQL Query Constraints

**SELECT * is NOT supported**
- Always explicitly list columns in queries
- DJ needs to know exact columns for type inference and validation

```sql
-- ❌ NOT ALLOWED
SELECT * FROM finance.transactions

-- ✅ CORRECT
SELECT transaction_id, user_id, amount_usd, transaction_date
FROM finance.transactions
```

**Column types are auto-inferred from queries**
- DJ analyzes your SQL and infers column types automatically
- No need to manually specify types in YAML

**Use CAST() to control types**
- If you need a specific type, use explicit CAST in your query

```sql
-- Force specific type
SELECT CAST(user_id AS bigint), CAST(revenue AS decimal(18,2))
FROM finance.transactions
```

### Metric Query Constraints

**Metrics cannot contain WHERE clauses**
- Use CASE WHEN for conditional aggregation instead

```sql
-- ❌ NOT ALLOWED - WHERE clause in metric
SELECT SUM(amount_usd)
FROM finance.transactions
WHERE status = 'completed'

-- ✅ CORRECT - Use CASE WHEN
SELECT SUM(
  CASE WHEN status = 'completed' THEN amount_usd ELSE 0 END
) FROM finance.transactions
```

**Metrics select a single expression from a single node**
- Cannot join multiple nodes in a metric query
- Define joins via dimension links on the source/transform node instead

**✨ Metrics can reference other metrics (composition)**
- Build derived metrics by referencing other metrics in the query
- Excellent for ratios, rates, and complex calculations
- Promotes reusability and maintainability

```sql
-- Create base metrics first
SELECT COUNT(*) FROM finance.transactions              -- metric: finance.transaction_count
SELECT SUM(amount_usd) FROM finance.transactions       -- metric: finance.total_revenue

-- Then compose them into a ratio metric
SELECT finance.total_revenue / finance.transaction_count  -- metric: finance.avg_transaction_value
-- Note: DJ automatically handles divide-by-zero, but you can add NULLIF() for extra safety
```

**Pattern: Build composable metrics**
1. Create focused base metrics (numerator, denominator)
2. Reference them in derived metrics for ratios
3. Easier to test, debug, and maintain

### Metric Ownership & Governance

**⚠️ ALWAYS specify owners for metrics**

Without clear ownership, metrics become unmaintainable:
- No one to contact when metric breaks or produces unexpected values
- Undefined responsibility for keeping metric definitions accurate
- Difficult to coordinate changes or deprecations
- Team knowledge scattered across individuals

**Best practices:**
- ✅ **Prefer team emails over individuals** - teams outlast individual contributors
- ✅ Use Google groups or distribution lists for teams
- ✅ Include both data team AND business stakeholder teams
- ❌ Never leave `owners` field empty or omit it

```yaml
# ✅ GOOD - Team ownership
owners:
  - data-platform-team@company.com
  - finance-analytics@company.com

# ⚠️ ACCEPTABLE - Individual ownership (less sustainable)
owners:
  - alice@company.com

# ❌ BAD - No owners (governance nightmare!)
# owners: []
```

---

## Working with DataJunction

### Discovery & Exploration (Use MCP Tools)

**When you need to explore the DJ semantic layer, use these MCP tools:**

#### Find Available Nodes

**Use MCP tool**: `search_nodes`
- Search for metrics, dimensions, cubes by name or namespace
- Filter by node type
- Returns list of matching nodes

**Example**: `search_nodes(query="revenue", node_type="metric", namespace="finance")`

#### Get Node Details

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

#### Check Common Dimensions

**Use MCP tool**: `get_common_dimensions`
- Find dimensions available across multiple metrics
- Essential before querying multiple metrics together
- Returns only dimensions shared by ALL specified metrics

**Example**: `get_common_dimensions(metric_names=["finance.total_revenue", "growth.daily_active_users"])`

**When to use**: Always check this before building queries with multiple metrics!

#### Get Node Lineage

**Use MCP tool**: `get_node_lineage`
- Get upstream dependencies (what data sources this uses)
- Get downstream dependencies (what will break if you change this)
- Direction: "upstream", "downstream", or "both"

**Example**: `get_node_lineage(node_name="finance.total_revenue", direction="both")`

---

### Querying & SQL Generation (Use MCP Tools)

**When you need to query metrics or generate SQL, use these MCP tools:**

#### Build Metric SQL

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

#### Get Metric Data

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

#### Visualize Metrics

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

### API Reference (Educational Context)

The MCP tools call the DJ REST and GraphQL APIs under the hood. Here's what they're doing:

#### REST API Endpoints

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

#### GraphQL API

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

---

## Building the Semantic Layer

### Two Ways to Contribute

**IMPORTANT**: Always validate with the user how they want to contribute before making changes.

#### Option 1: Direct API Changes (Immediate)

**How it works:**
- POST JSON to DJ API endpoints (or use the DJ UI, which makes the same API calls)
- Changes take effect immediately in DJ
- No git operations needed

**Good for:**
- Quick iterations and exploration
- Ad-hoc analysis
- Namespaces without repo backing

**⚠️ CONSTRAINT**: If a namespace is repo-backed and configured as read-only, direct API changes are **not allowed**. You must use the git workflow (Option 2).

**Example - Create a Metric**:
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.revenue",
    "description": "Total revenue from transactions",
    "query": "SELECT SUM(amount) AS revenue FROM finance.transactions",
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": "dollar"
    },
    "mode": "published"
  }'
```

#### Option 2: Repo-Backed Workflow (Version Controlled)

**How it works:**
- Write YAML node definitions in git repo
- Create feature branches for changes
- Submit pull requests for review
- Merge to default branch
- DJ syncs changes automatically

**Good for:**
- Production changes requiring review
- Team collaboration
- Maintaining history and audit trail
- Multi-step changes across many nodes

**REQUIRED for:**
- Namespaces configured as repo-backed and read-only

**See detailed workflow below in "Repo-Backed Workflow" section.**

---

### Checking if Namespace is Repo-Backed

Before making changes, check if the namespace uses git workflow and whether it's read-only.

**Best approach - Use MCP tool `get_node_details`:**

Query any node in the namespace to get git repository info:

```
get_node_details(name="finance.total_revenue")
```

The response will include git repository information:
```
Git Repository:
  Repo: owner/dj-finance
  Branch: main
  Default Branch: main
  → This namespace is repo-backed (use git workflow for changes)
```

**Alternative - REST API (shows read-only status):**
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/git

# Response:
{
  "github_repo_path": "owner/dj-finance",
  "git_branch": "main",
  "default_branch": "main",
  "git_path": "nodes/",
  "git_only": true    ← If true, namespace is read-only (API changes blocked)
}
```

**Alternative - GraphQL:**
```graphql
query {
  findNodes(names: ["finance.total_revenue"]) {
    name
    gitInfo {
      repo
      branch
      defaultBranch
    }
  }
}
```

**Decision tree:**
- **If git info is present AND `git_only: true`**: MUST use repo workflow (API changes will fail)
- **If git info is present AND `git_only: false`**: Can use either workflow
- **If git info is null**: Use API workflow (direct POST/PATCH)

---

### Creating Metrics (API Approach)

> **Important**: Metrics select a **single expression** from a **single source, transform, or dimension node**. They cannot contain WHERE clauses - use conditional aggregations instead.

#### Metric Structure

```sql
SELECT <aggregation_expression> AS <metric_name>
FROM <single_node>
```

#### Metric Metadata Fields

**Required fields:**
- `name` - Fully qualified metric name (e.g., `finance.total_revenue`)
- `query` - SQL aggregation expression

**Optional but recommended:**
- `description` - Human-readable description of what this metric measures
- `metric_metadata.direction` - Performance direction
  - `higher_is_better` - Revenue, user count, engagement
  - `lower_is_better` - Churn rate, error rate, latency
  - `neutral` - No inherent good/bad direction
- `metric_metadata.unit` - Measurement unit
  - `dollar` - Monetary values
  - `unitless` - Pure numbers, percentages, ratios
  - **⚠️ Do NOT use `count`** - The server will reject it, use `unitless` instead
- `mode` - Lifecycle state (`draft` or `published`)
- `required_dimensions` - Dimensions required for this metric to make sense (e.g., time-based metrics need a date dimension)

#### Simple Aggregation Metrics

**COUNT**:
```bash
curl -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.num_transactions",
    "description": "Total number of transactions",
    "query": "SELECT COUNT(transaction_id) AS num_transactions FROM finance.transactions",
    "mode": "published"
  }'
```

**SUM**:
```bash
curl -X POST $DJ_URL/nodes/metric/ \
  -d '{
    "name": "finance.total_revenue",
    "description": "Total revenue from all transactions",
    "query": "SELECT SUM(amount_usd) AS total_revenue FROM finance.transactions",
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": "dollar"
    },
    "mode": "published"
  }'
```

#### Conditional Aggregations

Since metrics cannot contain WHERE clauses, use **CASE WHEN** for filtering:

```bash
curl -X POST $DJ_URL/nodes/metric/ \
  -d '{
    "name": "finance.completed_revenue",
    "description": "Revenue from completed non-refund transactions",
    "query": "
      SELECT SUM(
        CASE
          WHEN status = '\''completed'\'' AND refund_flag = false
          THEN amount_usd
          ELSE 0
        END
      ) AS completed_revenue
      FROM finance.transactions
    ",
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": "dollar"
    },
    "mode": "published"
  }'
```

#### Ratio Metrics

DJ automatically handles divide-by-zero, but you can add NULLIF for extra safety:

```bash
curl -X POST $DJ_URL/nodes/metric/ \
  -d '{
    "name": "finance.refund_rate",
    "description": "Percentage of transactions that are refunded",
    "query": "
      SELECT
        COUNT_IF(refund_flag = true) * 1.0 / COUNT(*)
      AS refund_rate
      FROM finance.transactions
    ",
    "metric_metadata": {
      "direction": "lower_is_better",
      "unit": "unitless"
    },
    "owners": ["data-platform-team@company.com"],
    "mode": "published"
  }'
```

---

### Supported Metric Types

This section provides comprehensive patterns for different metric types you can build in DataJunction.

#### Base Metrics (Simple Aggregations)

**COUNT**
```yaml
name: finance.num_transactions
query: SELECT COUNT(transaction_id) FROM finance.transactions
```

**COUNT DISTINCT**
```yaml
name: finance.unique_customers
query: SELECT COUNT(DISTINCT customer_id) FROM finance.transactions
```

**APPROX_COUNT_DISTINCT** (HyperLogLog - for large datasets)
```yaml
name: finance.approx_unique_profiles
query: SELECT APPROX_COUNT_DISTINCT(profile_id) FROM finance.transactions
```

**SUM**
```yaml
name: finance.total_revenue
query: SELECT SUM(amount_usd) FROM finance.transactions
metric_metadata:
  direction: higher_is_better
  unit: dollar
```

**AVG**
```yaml
name: finance.avg_transaction_value
query: SELECT AVG(amount_usd) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: dollar
```

**Conditional Aggregation**
```yaml
name: finance.completed_revenue
description: Revenue from completed non-refund transactions only
query: |-
  SELECT SUM(
    CASE
      WHEN status = 'completed' AND refund_flag = false
      THEN amount_usd
      ELSE 0
    END
  ) FROM finance.transactions
metric_metadata:
  direction: higher_is_better
  unit: dollar
```

#### Derived Metrics (Reference Other Metrics)

**💡 Key Pattern: Metrics can reference other metrics!**

Build composable metrics by creating base metrics first, then referencing them in derived calculations. This makes your metrics easier to understand, test, and maintain.

**Ratio/Rate (Cross-Metric Calculation)**
```yaml
# First, create the base metrics
name: finance.clicks
query: SELECT COUNT(*) FROM finance.transactions WHERE event = 'click'
owners:
  - marketing-analytics@company.com

name: finance.impressions
query: SELECT COUNT(*) FROM finance.transactions WHERE event = 'impression'
owners:
  - marketing-analytics@company.com

# Then compose them into a ratio
name: finance.conversion_rate
description: Click-through rate as percentage
query: SELECT finance.clicks * 100.0 / finance.impressions
# Note: DJ handles divide-by-zero automatically
metric_metadata:
  direction: higher_is_better
  unit: unitless
owners:
  - marketing-analytics@company.com
```

**Revenue Per Metric**
```yaml
# Base metrics
name: finance.total_revenue
query: SELECT SUM(amount_usd) FROM finance.transactions
owners:
  - finance-data-team@company.com

name: finance.impressions
query: SELECT COUNT(*) FROM finance.ad_events WHERE event = 'impression'
owners:
  - marketing-analytics@company.com

# Composed ratio metric
name: finance.revenue_per_thousand_impressions
description: RPM (revenue per thousand impressions)
query: SELECT finance.total_revenue / finance.impressions * 1000
metric_metadata:
  direction: higher_is_better
  unit: dollar
owners:
  - finance-data-team@company.com
  - marketing-analytics@company.com
```

**Why this pattern is powerful:**
- ✅ Base metrics can be queried independently
- ✅ Easier to debug (check numerator and denominator separately)
- ✅ Reusable components (base metrics used in multiple ratios)
- ✅ Self-documenting (metric names explain the calculation)

#### Statistical Metrics

**Variance**
```yaml
name: finance.transaction_variance
query: SELECT VAR_POP(amount_usd) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: unitless
```

**Standard Deviation**
```yaml
name: finance.transaction_stddev
query: SELECT STDDEV_POP(amount_usd) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: unitless
```

**Percentiles (Approximate)**
```yaml
# Median (p50)
name: finance.median_transaction_value
query: SELECT PERCENTILE_APPROX(amount_usd, 0.5) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: dollar

# p90
name: finance.p90_transaction_value
query: SELECT PERCENTILE_APPROX(amount_usd, 0.9) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: dollar

# p95
name: finance.p95_transaction_value
query: SELECT PERCENTILE_APPROX(amount_usd, 0.95) FROM finance.transactions
metric_metadata:
  direction: neutral
  unit: dollar
```

#### Rolling Window Metrics

**Trailing 7-Day Sum**
```yaml
name: finance.trailing_7d_revenue
description: Sum of revenue over trailing 7-day window
query: |-
  SELECT SUM(finance.daily_revenue) OVER (
    ORDER BY common.dimensions.date.dateint
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  )
required_dimensions:
  - common.dimensions.date.dateint
metric_metadata:
  direction: higher_is_better
  unit: dollar
```

**Trailing 28-Day Average**
```yaml
name: finance.trailing_28d_avg_revenue
description: Average daily revenue over trailing 28 days
query: |-
  SELECT AVG(finance.daily_revenue) OVER (
    ORDER BY common.dimensions.date.dateint
    ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
  )
required_dimensions:
  - common.dimensions.date.dateint
metric_metadata:
  direction: higher_is_better
  unit: dollar
```

**Trailing 28-Day Rate**
```yaml
name: finance.trailing_28d_conversion_rate
description: Conversion rate over trailing 28-day window
query: |-
  SELECT
    SUM(finance.clicks) OVER (
      ORDER BY common.dimensions.date.dateint
      ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) * 100.0 /
    SUM(finance.impressions) OVER (
      ORDER BY common.dimensions.date.dateint
      ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    )
required_dimensions:
  - common.dimensions.date.dateint
metric_metadata:
  direction: higher_is_better
  unit: unitless
owners:
  - marketing-analytics@company.com
```

#### Period-over-Period Metrics

**Week-over-Week Change (Absolute)**
```yaml
name: finance.wow_revenue_change
description: Absolute change in revenue compared to previous week
query: |-
  SELECT
    finance.weekly_revenue -
    LAG(finance.weekly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.week_code
    )
required_dimensions:
  - common.dimensions.date.week_code
metric_metadata:
  direction: neutral
  unit: dollar
```

**Week-over-Week % Change**
```yaml
name: finance.wow_revenue_pct_change
description: Percentage change in revenue compared to previous week
query: |-
  SELECT
    (finance.weekly_revenue - LAG(finance.weekly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.week_code
    )) * 100.0 /
    LAG(finance.weekly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.week_code
    )
required_dimensions:
  - common.dimensions.date.week_code
metric_metadata:
  direction: neutral
  unit: unitless
owners:
  - finance-data-team@company.com
```

**Month-over-Month % Change**
```yaml
name: finance.mom_revenue_pct_change
description: Percentage change in revenue compared to previous month
query: |-
  SELECT
    (finance.monthly_revenue - LAG(finance.monthly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.month_code
    )) * 100.0 /
    LAG(finance.monthly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.month_code
    )
required_dimensions:
  - common.dimensions.date.month_code
metric_metadata:
  direction: neutral
  unit: unitless
owners:
  - finance-data-team@company.com
```

**Quarter-over-Quarter % Change**
```yaml
name: finance.qoq_revenue_pct_change
description: Percentage change in revenue compared to previous quarter
query: |-
  SELECT
    (finance.quarterly_revenue - LAG(finance.quarterly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.quarter_code
    )) * 100.0 /
    LAG(finance.quarterly_revenue, 1) OVER (
      ORDER BY common.dimensions.date.quarter_code
    )
required_dimensions:
  - common.dimensions.date.quarter_code
metric_metadata:
  direction: neutral
  unit: unitless
owners:
  - finance-data-team@company.com
```

**Year-over-Year % Change**
```yaml
name: finance.yoy_revenue_pct_change
description: Percentage change in revenue compared to same period last year
query: |-
  SELECT
    (finance.revenue - LAG(finance.revenue, 1) OVER (
      ORDER BY common.dimensions.date.year
    )) * 100.0 /
    LAG(finance.revenue, 1) OVER (
      ORDER BY common.dimensions.date.year
    )
required_dimensions:
  - common.dimensions.date.year
metric_metadata:
  direction: neutral
  unit: unitless
owners:
  - finance-data-team@company.com
```

#### Key Patterns Summary

- **DJ handles divide-by-zero automatically** - NULLIF() is optional but can be used for extra safety
- **Use CASE WHEN** instead of WHERE clauses for filtering
- **Window functions** enable rolling windows and period-over-period comparisons
- **required_dimensions** should include the dimension used in window ORDER BY clauses
- **Derived metrics** can reference other metrics for ratios and calculations
- **Always specify owners** - use team emails for better sustainability

#### Metric Metadata Quick Reference

| Field | Required | Valid Values | Notes |
|-------|----------|--------------|-------|
| `name` | ✅ Yes | `namespace.metric_name` | Fully qualified name |
| `query` | ✅ Yes | SQL SELECT expression | Single aggregation from single node |
| `description` | ❌ Optional | String | Recommended for clarity |
| `direction` | ❌ Optional | `higher_is_better`<br>`lower_is_better`<br>`neutral` | Indicates performance direction |
| `unit` | ❌ Optional | `dollar`<br>`unitless`<br>**⚠️ NOT `count`** | Server rejects `count` - use `unitless` |
| `mode` | ❌ Optional | `draft`<br>`published` | Default: `published` |
| `required_dimensions` | ❌ Optional | List of dimension names | For time-based metrics, windowed metrics |

---

## Repo-Backed Workflow

### Overview

DataJunction supports **repo-backed namespaces** where node definitions are stored as YAML files in a git repository. This enables:

- ✅ **Version control** for your semantic layer
- ✅ **Pull request review workflows** for changes
- ✅ **Branch-based development** (feature branches, environments)
- ✅ **Declarative configuration** (infrastructure as code)
- ✅ **Audit trail** of all changes
- ✅ **Team collaboration** with code review

---

### Repository Structure

The repository should follow this structure:

```
dj-finance/
├── nodes/
│   ├── sources/
│   │   ├── transactions.yaml
│   │   └── users.yaml
│   ├── dimensions/
│   │   ├── date.yaml
│   │   └── user.yaml
│   ├── metrics/
│   │   ├── revenue.yaml
│   │   └── user_count.yaml
│   └── transforms/
│       └── clean_transactions.yaml
├── cubes/
│   └── revenue_cube.yaml
└── README.md
```

---

### Branch-Based Development

#### Understanding Branch-Based Namespaces

When you create a branch in a repo-backed namespace, DJ creates a **corresponding namespace** that points to that branch.

**Naming convention:**
```
{namespace}.{branch_name}
```

**Examples:**
- `finance.main` → points to `main` branch of finance repo
- `finance.feature-new-metrics` → points to `feature-new-metrics` branch
- `finance.staging` → points to `staging` branch

#### Creating a Feature Branch

**Option A: Via DJ API**
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/branches \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "feature-new-metrics",
    "from_branch": "main"
  }'

# This creates:
# 1. Git branch: feature-new-metrics (from main)
# 2. DJ namespace: finance.feature-new-metrics
```

**Option B: Manually in Git**
```bash
cd dj-finance/
git checkout -b feature-new-metrics
git push origin feature-new-metrics

# DJ may auto-discover branches
```

---

### YAML Node Definitions

All node types can be defined as YAML files in the repository.

#### Column-Level Configuration

**Column types are auto-inferred from queries**
- DJ analyzes your SQL and automatically determines column types
- No need to manually specify types in the `columns` section

**Use CAST() to control specific types**
```yaml
query: |
  SELECT
    CAST(user_id AS bigint) AS user_id,
    CAST(revenue AS decimal(18,2)) AS revenue,
    transaction_date
  FROM finance.transactions
```

**Column attributes**
- `primary_key` - Marks column as part of the primary key
- `dimension` - Marks column as available for grouping (transforms/facts only)

**Optional column fields**
- `display_name` - Human-readable name for UI display
  ```yaml
  columns:
    - name: ad_account_id
      display_name: "Ad Account ID"
      attributes:
        - dimension
  ```

**Important constraints:**
- ❌ **SELECT * is NOT supported** - Always explicitly list columns
- ✅ Column order in YAML should match SELECT order
- ✅ Use descriptive display names for better UX

#### Source Node YAML

```yaml
# nodes/sources/transactions.yaml
name: finance.transactions
description: Raw transaction data from payment system
type: source
catalog: prod_catalog
schema_: finance
table: transactions_table

columns:
  - name: transaction_id
    display_name: "Transaction ID"
    attributes:
      - primary_key
  - name: user_id
    display_name: "User ID"
  - name: amount_usd
    display_name: "Amount (USD)"
  - name: transaction_date
    display_name: "Transaction Date"
  - name: status
    display_name: "Status"

dimension_links:
  - dimension: common.dimensions.users
    join_type: left
    join_on: finance.transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_type: left
    join_on: finance.transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

**Notes:**
- Column types are auto-inferred from the source table schema
- `display_name` provides human-readable labels for UI
- `join_type` can be `left`, `right`, or `inner` (defaults to `left`)

#### Dimension Node YAML

```yaml
# nodes/dimensions/user.yaml
name: finance.user
description: User dimension with attributes
type: dimension
query: |
  SELECT
    user_id,
    username,
    email,
    country_code,
    signup_date,
    tier
  FROM finance.users

primary_key:
  - user_id

columns:
  - name: user_id
    display_name: "User ID"
    attributes:
      - primary_key
  - name: username
    display_name: "Username"
  - name: email
    display_name: "Email Address"
  - name: country_code
    display_name: "Country"
  - name: signup_date
    display_name: "Signup Date"
  - name: tier
    display_name: "User Tier"

dimension_links:
  - dimension: common.dimensions.date
    join_type: left
    join_on: finance.user.signup_date = common.dimensions.date.dateint

  - dimension: common.dimensions.country
    join_type: left
    join_on: finance.user.country_code = common.dimensions.country.country_code

mode: published
```

**Notes:**
- Dimension node columns don't need `dimension` attribute (all are dimensions by nature)
- `display_name` improves readability in query builders and dashboards

#### Metric Node YAML

```yaml
# nodes/metrics/revenue.yaml
name: finance.total_revenue
description: Total revenue from completed transactions
type: metric
query: |
  SELECT
    SUM(
      CASE
        WHEN status = 'completed' AND refund_flag = false
        THEN amount_usd
        ELSE 0
      END
    ) AS total_revenue
  FROM finance.transactions

required_dimensions:
  - common.dimensions.date.dateint

metric_metadata:
  direction: higher_is_better
  unit: dollar

owners:
  - data-platform-team@company.com  # Ideally use teams for sustainability
  - alice@company.com               # Can also specify individuals

mode: published
```

**Important metric rules:**
- ✅ **ALWAYS specify owners** - Critical for governance! Use team emails for sustainability
- ❌ **No WHERE clauses** in metric queries (use CASE WHEN instead)
- ✅ **Use CASE WHEN** for conditional aggregation
- ✅ **Include required_dimensions** for time-based metrics
- ✅ **Add metric_metadata** for direction and unit

#### Transform Node YAML

```yaml
# nodes/transforms/clean_transactions.yaml
name: finance.clean_transactions
description: Cleaned transaction data with standardized status
type: transform
primary_key:
  - transaction_id
query: |
  SELECT
    transaction_id,
    user_id,
    amount_usd,
    transaction_date,
    CASE
      WHEN status IN ('complete', 'completed', 'success') THEN 'completed'
      WHEN status IN ('fail', 'failed', 'error') THEN 'failed'
      ELSE status
    END AS status_clean,
    refund_flag
  FROM finance.transactions

columns:
  - name: transaction_id
    display_name: "Transaction ID"
    attributes:
      - primary_key
  - name: user_id
    display_name: "User ID"
    attributes:
      - dimension
  - name: amount_usd
    display_name: "Amount (USD)"
  - name: transaction_date
    display_name: "Transaction Date"
    attributes:
      - dimension
  - name: status_clean
    display_name: "Status"
    attributes:
      - dimension
  - name: refund_flag
    display_name: "Refund Flag"
    attributes:
      - dimension

dimension_links:
  - dimension: common.dimensions.users
    join_type: left
    join_on: finance.clean_transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_type: left
    join_on: finance.clean_transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

**Notes:**
- `primary_key` field lists the primary key column(s)
- `dimension` attribute marks columns available for grouping in metrics
- Column types are auto-inferred from the query
- Columns without `dimension` attribute are typically measures/facts

#### Cube YAML

```yaml
# cubes/revenue_cube.yaml
name: finance.revenue_cube
description: Pre-computed revenue metrics by date and region
metrics:
  - finance.total_revenue
  - finance.avg_transaction_value

dimensions:
  - common.dimensions.date.dateint
  - common.dimensions.date.month
  - common.dimensions.users.country_code

mode: published
```

**When to use cubes:**
- Frequently queried metric combinations
- Pre-compute for performance (materialization)
- Dashboard metric sets

**Critical**: All metrics in a cube MUST share ALL dimensions in the cube. Use `get_common_dimensions` MCP tool to check first!

---

### Temporal Partitions on Cubes

**Temporal partitions** enable automatic partition filtering for performance optimization. When configured, DJ automatically adds partition filters to SQL queries, dramatically improving query performance on large datasets.

#### How Temporal Partitions Work

When you set a temporal partition on a cube, DJ will:
1. Generate SQL with `${dj_logical_timestamp}` template variables in partition filters
2. These template variables get replaced with actual timestamp values at query execution time
3. Push down these filters to all upstream nodes that have the same dimension linked
4. Reduce data scanned by limiting to only relevant partitions based on the time range

#### Configuring Temporal Partitions

**Cube YAML with temporal partition:**
```yaml
# cubes/revenue_cube.yaml
name: finance.revenue_cube
description: Pre-computed revenue metrics by date and region
metrics:
  - finance.total_revenue
  - finance.avg_transaction_value

dimensions:
  - common.dimensions.date.dateint
  - common.dimensions.date.month
  - common.dimensions.users.country_code

# Temporal partition configuration
temporal_partition:
  dimension_attribute: common.dimensions.date.dateint
  granularity: day

mode: published
```

**Temporal partition fields:**
- `dimension_attribute` - The dimension attribute used for partitioning (typically a date field)
- `granularity` - Time granularity: `day`, `month`, `quarter`, `year`

#### Requirements for Partition Filtering

For DJ to generate partition filters, **all upstream nodes** (sources, transforms, dimensions) must:
1. Have the **same dimension linked** that's used in the temporal partition
2. Use the **same join key** (e.g., `dateint`)

**Example - Upstream node with matching dimension link:**
```yaml
# nodes/sources/transactions.yaml
name: finance.transactions
type: source
# ...

dimension_links:
  - dimension: common.dimensions.date
    join_on: finance.transactions.transaction_date = common.dimensions.date.dateint
    # ↑ This matches the temporal_partition.dimension_attribute in the cube
```

**What happens:**
- ✅ If cube has `temporal_partition.dimension_attribute: common.dimensions.date.dateint`
- ✅ And upstream node links to `common.dimensions.date` on `dateint`
- ✅ Then DJ automatically adds partition filters like `WHERE transaction_date >= X AND transaction_date <= Y`

**What if dimension links don't match:**
- ❌ Cube has temporal partition on `common.dimensions.date.dateint`
- ❌ But upstream node doesn't link to `common.dimensions.date`
- ❌ Result: No automatic partition filtering, full table scan!

#### Regular Filters vs Temporal Filters

**Regular filters** - Use when you want direct, explicit filter values:
```
build_metric_sql(
  metrics=["finance.total_revenue"],
  dimensions=["common.dimensions.date.dateint"],
  filters=["common.dimensions.date.dateint = 20240101"]  # Direct filter value
)
```

**Generated SQL:**
```sql
SELECT SUM(amount_usd) AS total_revenue, transaction_date
FROM finance.transactions
WHERE transaction_date = 20240101  -- ← Direct filter value
GROUP BY transaction_date
```

**Temporal filters** - Use when you want template variables for incremental processing:
```
build_metric_sql(
  metrics=["finance.total_revenue"],
  dimensions=["common.dimensions.date.dateint"],
  include_temporal_filters=True,  # Enable temporal filter template generation
  lookback_window="7 DAY"          # Optional: lookback window
)
```

**Generated SQL:**
```sql
SELECT SUM(amount_usd) AS total_revenue, transaction_date
FROM finance.transactions
WHERE transaction_date >= ${dj_logical_timestamp}  -- ← Template variable
  AND transaction_date <= ${dj_logical_timestamp}
GROUP BY transaction_date
```

**When to use temporal filters:**
- Materialization jobs that run incrementally
- Scheduled queries that need dynamic time ranges
- Pre-aggregation pipelines

**When to use regular filters:**
- Ad-hoc queries with specific date ranges
- One-time analysis
- When you know the exact filter values

**How temporal filters work:**
- `include_temporal_filters=True` generates SQL with `${dj_logical_timestamp}` template variables
- These placeholders get replaced with actual timestamp values at query execution time
- `lookback_window` parameter controls the time range (e.g., '3 DAY', '1 WEEK', '30 DAY')
- The actual filter values are calculated based on cube's temporal partition configuration and execution time

#### Best Practices for Temporal Partitions

1. **Always set temporal partitions on cubes used for dashboards**
   - Dramatically improves query performance
   - Reduces data scanned

2. **Ensure consistent dimension links across all nodes**
   - Check that all upstream sources/transforms link to the same date dimension
   - Use the same join key (e.g., always `dateint`, not mixing `dateint` and `date_str`)

3. **Use appropriate granularity**
   - `day` - For daily metrics and dashboards (most common)
   - `month` - For monthly aggregations
   - `quarter`, `year` - For higher-level reporting

4. **Verify partition filtering is working**
   - Use `build_metric_sql` with `include_temporal_filters=True`
   - Check generated SQL includes partition filters on upstream tables
   - If filters missing, check dimension link consistency

5. **Match physical partition scheme**
   - If your data warehouse partitions by `date`, use `dateint` in temporal partition
   - Align with how data is actually partitioned in storage

#### Example: Complete Temporal Partition Setup

**Step 1: Source with date dimension link**
```yaml
# nodes/sources/orders.yaml
name: ecommerce.orders
type: source
catalog: prod
schema_: ecommerce
table: orders_partitioned

dimension_links:
  - dimension: common.dimensions.date
    join_on: ecommerce.orders.order_date = common.dimensions.date.dateint
```

**Step 2: Transform with same date dimension link**
```yaml
# nodes/transforms/daily_orders.yaml
name: ecommerce.daily_orders
type: transform
query: |
  SELECT
    product_id,
    order_date,
    COUNT(*) AS order_count,
    SUM(amount_usd) AS total_revenue
  FROM ecommerce.orders
  GROUP BY product_id, order_date

dimension_links:
  - dimension: common.dimensions.date
    join_on: ecommerce.daily_orders.order_date = common.dimensions.date.dateint
```

**Step 3: Metrics on the transform**
```yaml
# nodes/metrics/total_orders.yaml
name: ecommerce.total_orders
type: metric
query: SELECT SUM(order_count) FROM ecommerce.daily_orders
```

**Step 4: Cube with temporal partition**
```yaml
# cubes/ecommerce_cube.yaml
name: ecommerce.ecommerce_cube
metrics:
  - ecommerce.total_orders
  - ecommerce.total_revenue
dimensions:
  - common.dimensions.date.dateint
  - ecommerce.daily_orders.product_id

temporal_partition:
  dimension_attribute: common.dimensions.date.dateint
  granularity: day
```

**Result**: Queries on `ecommerce.ecommerce_cube` will automatically include partition filters on both `ecommerce.orders` and `ecommerce.daily_orders` tables!

---

### Complete Workflow Example

**Scenario**: Add a new metric to the finance namespace

**Step 1: Check if finance is repo-backed**

Use MCP tool:
```
get_node_details(name="finance.total_revenue")

# Check the output for git repository info
# If "Git Repository" section is present → Use repo workflow!
```

Or via REST API:
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/git
```

**Step 2: Create feature branch**
```bash
# Via DJ API
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/branches \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "feature-add-churn-metric",
    "from_branch": "main"
  }'

# Creates namespace: finance.feature-add-churn-metric
```

**Step 3: Clone repo and checkout branch**
```bash
git clone https://github.com/myorg/dj-finance.git
cd dj-finance
git checkout feature-add-churn-metric
```

**Step 4: Create metric YAML file**
```bash
cat > nodes/metrics/churn_rate.yaml <<EOF
name: finance.churn_rate
description: Monthly user churn rate
type: metric
query: |
  SELECT
    CAST(SUM(CASE WHEN churned = true THEN 1 ELSE 0 END) AS DOUBLE) /
    COUNT(DISTINCT user_id) AS churn_rate
  FROM finance.user_activity

required_dimensions:
  - common.dimensions.date.month

metric_metadata:
  direction: lower_is_better
  unit: percentage

owners:
  - growth-analytics@company.com

mode: published
EOF
```

**Step 5: Commit and push**
```bash
git add nodes/metrics/churn_rate.yaml
git commit -m "Add monthly churn rate metric"
git push origin feature-add-churn-metric
```

**Step 6: DJ syncs automatically**

Use MCP tool to verify:
```
get_node_details(name="finance.churn_rate")
```

Or via API:
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.churn_rate
```

**Step 7: Test in branch namespace**

Use MCP tool:
```
build_metric_sql(
  metrics=["finance.churn_rate"],
  dimensions=["common.dimensions.date.month"],
  filters=["common.dimensions.date.year = 2024"]
)
```

**Step 8: Create PR for review**
```bash
gh pr create \
  --title "Add monthly churn rate metric" \
  --body "Adds a new metric to track monthly user churn rate" \
  --base main
```

**Step 9: Get review, merge PR**
```bash
# After approval, merge via GitHub UI or:
gh pr merge --squash
```

**Step 10: Changes sync to production**

Metric now available in `finance.main` namespace!

---

## Best Practices

### Naming Conventions

Use fully qualified names with namespace:

```
namespace.node_name
```

**Examples:**
- ✅ `finance.total_revenue`
- ✅ `common.dimensions.users`
- ✅ `clean.user_events`
- ❌ `revenue` (missing namespace)

### Namespace Organization

Namespaces are organized by business area:

**Common conventions:**
- `common.dimensions.*` - Shared dimensions (users, dates, regions)
- `finance.*` - Financial metrics & facts
- `growth.*` - User engagement & activation
- `product.*` - Product usage & features
- `source.*` - Raw source tables

### When to Use Repo Workflow vs API

**✅ MUST use repo workflow for:**
- Namespaces configured as repo-backed and read-only (`git_only: true`)

**✅ Should use repo workflow for:**
- Production changes (require review)
- Multi-node changes (related metrics/dimensions)
- Team environments (multiple contributors)
- Changes requiring audit trail
- Complex refactoring

**✅ Can use API workflow for:**
- Quick exploration and prototyping
- Ad-hoc analysis
- Single-user namespaces
- Temporary metrics
- Non-production experiments
- **Only if namespace is not read-only repo-backed**

### Metric Best Practices

- ✅ **ALWAYS assign owners** - This is critical for governance! Use team emails rather than individuals for better continuity
- ✅ Select single expression from single node
- ✅ Use CASE WHEN for filtering (no WHERE clauses)
- ✅ **Build composable metrics** - create base metrics (numerator/denominator), then reference them in ratios
- ✅ Add metric metadata (direction, unit)
- ✅ **Always use `unitless` for counts/ratios** - never use `count` (server rejects it)
- ✅ Specify `required_dimensions` for time-based metrics
- ✅ Test with MCP tools before publishing
- ✅ Use descriptive names that indicate what's being measured

**Note**: DJ automatically handles divide-by-zero, so NULLIF() is optional (you can add it for extra safety if desired).

### Column Best Practices

- ✅ Always explicitly list columns (SELECT * not supported)
- ✅ Add `display_name` for better UX in query builders
- ✅ Mark primary key columns with `primary_key` attribute
- ✅ Mark groupable columns with `dimension` attribute (transforms/facts)
- ✅ Use CAST() in query if you need specific types
- ✅ Column order in YAML should match SELECT order

### Cube Best Practices

- ✅ Use `get_common_dimensions` MCP tool to check compatibility first
- ✅ Only use shared dimensions
- ✅ Use for frequently queried combinations
- ✅ **Always set temporal partitions on cubes** for performance
- ✅ Ensure all upstream nodes link to the same date dimension
- ✅ Use `include_temporal_filters=True` when generating SQL
- ✅ Match granularity to physical partition scheme in data warehouse
- ✅ Verify partition filters in generated SQL

### Workflow Tips

1. **Always create feature branches** - never commit directly to default branch
2. **Use descriptive branch names** - `feature-add-revenue-metrics` not `fix-stuff`
3. **Write clear commit messages** - explain the "why" not just the "what"
4. **Keep PRs focused** - one logical change per PR
5. **Test in branch namespace** - validate metrics work before merging
6. **Use draft mode first** - set `mode: draft` while developing, `published` when ready
7. **Document in YAML** - use `description` fields thoroughly
8. **Assign ownership** - ideally teams rather than individuals for better continuity

---

## Key Concepts Summary

| Concept | Description |
|---------|-------------|
| **Node** | Any entity in DJ (source, dimension, metric, etc.) |
| **Dimension Link** | Defines how nodes join to dimensions |
| **Dimensional Graph** | Network of dimensions linked to each other |
| **Star Schema** | Fact tables at center, dimension tables radiate out |
| **Auto-Join** | DJ automatically finds join paths via dimension links |
| **Mode** | User-controlled: draft vs published |
| **Status** | System-controlled: valid vs invalid |
| **Namespace** | Logical grouping (finance, core, growth) |
| **Repo-Backed** | Namespace definitions stored in git as YAML |
| **Branch Namespace** | DJ namespace pointing to specific git branch |

---

## Additional Resources

- **DJ Documentation**: https://datajunction.io
- **GitHub**: https://github.com/DataJunction/dj
- **API Docs**: {server_url}/docs (Swagger UI)
- **GraphQL Playground**: {server_url}/graphql
