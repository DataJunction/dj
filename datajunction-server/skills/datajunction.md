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
  dialect="trino"
)
```

**Returns**:
- Generated SQL for specified engine
- Output columns with types
- Cube name (if materialized cube used)

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
- POST JSON to DJ API endpoints
- Changes take effect immediately in DJ
- No git operations needed

**Good for:**
- Quick iterations and exploration
- Ad-hoc analysis
- Namespaces without repo backing

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

**See detailed workflow below in "Repo-Backed Workflow" section.**

---

### Checking if Namespace is Repo-Backed

Before making changes, check if the namespace uses git workflow:

```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance

# Response includes:
{
  "namespace": "finance",
  "repo_url": "https://github.com/myorg/dj-finance.git",  # ← Repo-backed!
  "default_branch": "main"
}
```

**If `repo_url` is present**: Use repo workflow (YAML definitions below)
**If `repo_url` is null**: Use API workflow (direct POST/PATCH)

---

### Creating Metrics (API Approach)

> **Important**: Metrics select a **single expression** from a **single source, transform, or dimension node**. They cannot contain WHERE clauses - use conditional aggregations instead.

#### Metric Structure

```sql
SELECT <aggregation_expression> AS <metric_name>
FROM <single_node>
```

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

Use **NULLIF** to avoid division by zero:

```bash
curl -X POST $DJ_URL/nodes/metric/ \
  -d '{
    "name": "finance.refund_rate",
    "description": "Percentage of transactions that are refunded",
    "query": "
      SELECT
        COUNT_IF(refund_flag = true) * 1.0 /
        NULLIF(COUNT(*), 0)
      AS refund_rate
      FROM finance.transactions
    ",
    "metric_metadata": {
      "direction": "lower_is_better",
      "unit": "proportion"
    },
    "mode": "published"
  }'
```

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
    type: bigint
  - name: user_id
    type: bigint
  - name: amount_usd
    type: decimal(10,2)
  - name: transaction_date
    type: date
  - name: status
    type: string

dimension_links:
  - dimension: common.dimensions.users
    join_on: finance.transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_on: finance.transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

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

dimension_links:
  - dimension: common.dimensions.date
    join_on: finance.user.signup_date = common.dimensions.date.dateint

  - dimension: common.dimensions.country
    join_on: finance.user.country_code = common.dimensions.country.country_code

mode: published
```

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

mode: published
```

**Important metric rules:**
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

dimension_links:
  - dimension: common.dimensions.users
    join_on: finance.clean_transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_on: finance.clean_transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

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

### Complete Workflow Example

**Scenario**: Add a new metric to the finance namespace

**Step 1: Check if finance is repo-backed**
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance

# Response shows:
# "repo_url": "https://github.com/myorg/dj-finance.git"
# → Use repo workflow!
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
    NULLIF(COUNT(DISTINCT user_id), 0) AS churn_rate
  FROM finance.user_activity

required_dimensions:
  - common.dimensions.date.month

metric_metadata:
  direction: lower_is_better
  unit: percentage

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

**✅ Use repo workflow for:**
- Production changes (require review)
- Multi-node changes (related metrics/dimensions)
- Team environments (multiple contributors)
- Changes requiring audit trail
- Complex refactoring

**✅ Use API workflow for:**
- Quick exploration and prototyping
- Ad-hoc analysis
- Single-user namespaces
- Temporary metrics
- Non-production experiments

### Metric Best Practices

- ✅ Select single expression from single node
- ✅ Use CASE WHEN for filtering (no WHERE clauses)
- ✅ Use NULLIF for divisions
- ✅ Add metric metadata (direction, unit)
- ✅ Test with MCP tools before publishing

### Cube Best Practices

- ✅ Use `get_common_dimensions` MCP tool to check compatibility first
- ✅ Only use shared dimensions
- ✅ Use for frequently queried combinations

### Workflow Tips

1. **Always create feature branches** - never commit directly to default branch
2. **Use descriptive branch names** - `feature-add-revenue-metrics` not `fix-stuff`
3. **Write clear commit messages** - explain the "why" not just the "what"
4. **Keep PRs focused** - one logical change per PR
5. **Test in branch namespace** - validate metrics work before merging
6. **Use draft mode first** - set `mode: draft` while developing, `published` when ready
7. **Document in YAML** - use `description` fields thoroughly

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
