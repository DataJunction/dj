---
name: datajunction-repo
description: |
  Activate this skill when authoring DataJunction (DJ) nodes via YAML files
  in a git repository — the repo-backed workflow. Covers YAML schemas per
  node type, branch-based development, temporal partitions on cubes, and
  the full PR-driven deployment flow. For modeling decisions (how to
  structure metrics, decomposition workflow), invoke `datajunction-semantic-model`.
  For direct API authoring, invoke `datajunction-api`. For concepts,
  invoke `datajunction`.
  Keywords:
  - YAML nodes, YAML definitions
  - repo-backed namespace, repo-backed workflow
  - git workflow, branch development, feature branch
  - cube YAML, metric YAML, dimension YAML, transform YAML
  - create metric, create dimension, create cube, build cube
  - temporal partition, partition pushdown
  - pre-commit, push.sh
user-invocable: false
---

# DataJunction Repo-Backed Workflow

YAML-and-git authoring for DJ. Use this skill when working in a repo-backed namespace — node definitions are YAML files, changes go through PRs, CI/CD deploys.

For modeling decisions (whether to make something a metric, dim, or transform; how to decompose a query), see `datajunction-semantic-model`. This skill assumes the modeling decisions are made and you're translating them into YAML.

## Overview

DataJunction supports **repo-backed namespaces** where node definitions are stored as YAML files in a git repository. This enables:

- ✅ **Version control** for your semantic layer
- ✅ **Pull request review workflows** for changes
- ✅ **Branch-based development** (feature branches, environments)
- ✅ **Declarative configuration** (infrastructure as code)
- ✅ **Audit trail** of all changes
- ✅ **Team collaboration** with code review

---

## Repository Structure

**There is no required layout under `nodes/`.** DJ walks the tree recursively (`rglob("nodes/**/*.yaml")`) and takes the namespace from each YAML's `name:` field. The folder path is purely organizational. Files can be flat, nested by type, nested by domain, or any mix — DJ doesn't care, and reorganizing later is a free `git mv`.

That said, you should still *choose* a layout for your repo so contributors know where to add things. A few common patterns:

**By node type** (good when most contributors are adding metrics across one domain):

```
nodes/
  sources/transactions.yaml
  dimensions/user.yaml
  metrics/revenue.yaml
  transforms/clean_transactions.yaml
cubes/revenue_cube.yaml
```

**By domain** (good when multiple teams/areas share one repo):

```
nodes/
  billing/
    sources/payments.yaml
    transforms/payment_events_clean.yaml
    metrics/monthly_revenue.yaml
  user_events/
    transforms/sessions.yaml
    metrics/dau.yaml
```

**Flat** (small projects, <30 nodes):

```
nodes/
  transactions.yaml
  user.yaml
  revenue.yaml
```

### Deciding on a layout

**For an existing repo:** look at the current `nodes/` tree and match it. Consistency within a repo matters more than any specific scheme. Don't introduce a new style alongside an established one.

**For a new repo:** ask the user a few questions before committing to a layout:
- How many contributors / teams will use this repo?
- Roughly how many nodes do you expect (10? 100? 1000?)
- Are most nodes going to be metrics on top of a few shared transforms, or many independent fact/dim chains?

Defaults from those answers:
- 1 team + <30 nodes → flat is fine
- 1 team + larger → by node type
- Multiple teams → by domain, with optional type subdirs inside each domain folder when a domain itself grows past ~30 nodes

The right call almost never matters as much as it feels like it does — folder layout has zero effect on deployed namespaces, so optimize for "where would a new contributor look for X" and accept that you'll reorganize as the repo grows.

---

## Branch-Based Development

### Understanding Branch-Based Namespaces

When you create a branch in a repo-backed namespace, DJ creates a **corresponding namespace** that points to that branch.

**Naming convention:**
```
{namespace}.{branch_name}
```

**Examples:**
- `finance.main` → points to `main` branch of finance repo
- `finance.feature-new-metrics` → points to `feature-new-metrics` branch
- `finance.staging` → points to `staging` branch

### Creating a Feature Branch

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

## YAML Node Definitions

All node types can be defined as YAML files in the repository.

### Column-Level Configuration

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
- `primary_key` — Marks column as part of the primary key
- `dimension` — Marks column as available for grouping (transforms/facts only)

**Optional column fields**
- `display_name` — Human-readable name for UI display
  ```yaml
  columns:
    - name: ad_account_id
      display_name: "Ad Account ID"
      attributes:
        - dimension
  ```

**Important constraints:**
- ❌ **SELECT * is NOT supported** — always explicitly list columns
- ✅ Column order in YAML should match SELECT order
- ✅ Use descriptive display names for better UX

### Source Node YAML

```yaml
# nodes/sources/transactions.yaml
name: finance.transactions
description: Raw transaction data from payment system
node_type: source
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
  - type: join
    dimension_node: common.dimensions.users
    join_type: left
    join_on: finance.transactions.user_id = common.dimensions.users.user_id

  - type: join
    dimension_node: common.dimensions.date
    join_type: left
    join_on: finance.transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

**Notes:**
- Column types are auto-inferred from the source table schema
- `display_name` provides human-readable labels for UI
- `join_type` can be `left`, `right`, or `inner` (defaults to `left`)

### Dimension Node YAML

```yaml
# nodes/dimensions/user.yaml
name: finance.user
description: User dimension with attributes
node_type: dimension
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
  - type: join
    dimension_node: common.dimensions.date
    join_type: left
    join_on: finance.user.signup_date = common.dimensions.date.dateint

  - type: join
    dimension_node: common.dimensions.country
    join_type: left
    join_on: finance.user.country_code = common.dimensions.country.country_code

mode: published
```

**Notes:**
- Dimension node columns don't need `dimension` attribute (all are dimensions by nature)
- `display_name` improves readability in query builders and dashboards

### Metric Node YAML

```yaml
# nodes/metrics/revenue.yaml
name: finance.total_revenue
description: Total revenue from completed transactions
node_type: metric
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
  - data-platform-team@company.com
  - alice@company.com

mode: published
```

**Important metric rules** (see `datajunction-semantic-model` for full discussion):
- ✅ **ALWAYS specify owners** — critical for governance
- ❌ **No WHERE clauses** in metric queries (use CASE WHEN instead)
- ✅ **Include required_dimensions** for time-based metrics
- ✅ **Add metric_metadata** for direction and unit

### Transform Node YAML

```yaml
# nodes/transforms/clean_transactions.yaml
name: finance.clean_transactions
description: Cleaned transaction data with standardized status
node_type: transform
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
  - type: join
    dimension_node: common.dimensions.users
    join_type: left
    join_on: finance.clean_transactions.user_id = common.dimensions.users.user_id

  - type: join
    dimension_node: common.dimensions.date
    join_type: left
    join_on: finance.clean_transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

**Notes:**
- `primary_key` field lists the primary key column(s)
- `dimension` attribute marks columns available for grouping in metrics
- Column types are auto-inferred from the query
- Columns without `dimension` attribute are typically measures/facts

### Cube YAML

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

**Critical**: All metrics in a cube MUST share ALL dimensions in the cube. Use `get_common_dimensions` MCP tool (see `datajunction-query`) to check first.

---

## Temporal Partitions on Cubes

**Temporal partitions** enable automatic partition filtering for performance optimization. When configured, DJ automatically adds partition filters to SQL queries, dramatically improving query performance on large datasets.

### How Partitions Work

A partition is always declared on a **column**. When that column is a dimension attribute on a cube, DJ uses it as the partition boundary and pushes the filter down to all upstream nodes that link to that same dimension.

**Partition field format:**
```yaml
partition:
  type: temporal        # or: categorical
  granularity: day      # second, minute, hour, day, week, month, quarter, year
  format: yyyyMMdd      # Java/Spark date format (e.g. yyyyMMdd → 20240101, yyyy-MM-dd → 2024-01-01)
```

### Declaring a Partition on a Cube

In a cube, declare the partition in the **`columns:` section** using the **full dimension attribute path** as the column name:

```yaml
# cubes/revenue_cube.yaml
name: ${prefix}revenue_cube
node_type: cube
metrics:
  - ${prefix}total_revenue
  - ${prefix}order_count

dimensions:
  - common.dimensions.date.dateint
  - common.dimensions.geo.country_code

columns:
  - name: common.dimensions.date.dateint   # ← must match exactly the entry in dimensions
    display_name: Date
    attributes:
      - primary_key
    partition:
      type: temporal
      granularity: day
      format: yyyyMMdd
```

### How Partition Filter Pushdown Works

Once a cube column has a partition spec, DJ:
1. Generates SQL with `${dj_logical_timestamp}` template variables when `include_temporal_filters=True`
2. Pushes those filters down to all upstream nodes that link to the same dimension
3. Reduces data scanned by limiting to relevant partitions

For filter pushdown to work, upstream nodes (sources, transforms) must have a **dimension link to the same dimension**:

```yaml
# transforms/orders.yaml
dimension_links:
  - type: join
    dimension_node: common.dimensions.date
    join_type: left
    join_on: ${prefix}orders.order_date = common.dimensions.date.dateint
    # ↑ DJ traces this link and pushes WHERE order_date >= X AND order_date <= Y
```

- ✅ Upstream node links to `common.dimensions.date` on `order_date` → DJ pushes `WHERE order_date >= X AND order_date <= Y`
- ❌ Upstream node has no link to `common.dimensions.date` → no filter pushed, full table scan

### Best Practices for Temporal Partitions

1. **Declare the partition on the cube's `columns:` block**
   - Use the full dimension attribute path as the column name (must match exactly what's in `dimensions:`)
   - Without a `partition:` declared on a cube column, DJ cannot enable partition filtering for that cube

2. **Ensure consistent dimension links across all nodes**
   - All upstream sources/transforms must link to the same dimension that carries the partition
   - Use the same join key everywhere (e.g., always `dateint`, not mixing `dateint` and `date_str`)

3. **Use appropriate granularity and format**
   - `granularity: day` with `format: yyyyMMdd` — for integer date partitions like `20240101`
   - `granularity: day` with `format: yyyy-MM-dd` — for string date partitions like `2024-01-01`
   - `granularity: month`, `quarter`, `year` — for coarser partitioning

4. **Verify partition filtering is working**
   - Use `build_metric_sql` (`datajunction-query` skill) with `include_temporal_filters=True`
   - Check generated SQL includes partition filters on upstream tables
   - If filters are missing, check that upstream nodes have dimension links pointing to the partitioned dimension column

5. **Match the physical partition scheme of your warehouse**
   - The `format` must match how partition values are actually stored in the table
   - Align granularity with how data is physically partitioned in storage

### Example: Complete Temporal Partition Setup

The partition is declared on the cube's `dateint` column. DJ pushes the filter down to `orders` because it has a dimension link to `common.dimensions.date`.

**Step 1: Transform with date dimension link**
```yaml
# transforms/orders.yaml
name: ${prefix}orders
node_type: transform
columns:
  - name: order_date
  - name: product_id
  - name: order_count
  - name: total_revenue

dimension_links:
  - type: join
    dimension_node: common.dimensions.time.date
    join_type: left
    join_on: ${prefix}orders.order_date = common.dimensions.time.date.dateint

query: |
  SELECT product_id, order_date, COUNT(*) AS order_count, SUM(amount_usd) AS total_revenue
  FROM source.prod.orders_f
  GROUP BY product_id, order_date
```

**Step 2: Metrics**
```yaml
# metrics/total_orders.yaml
name: ${prefix}total_orders
node_type: metric
query: SELECT SUM(order_count) FROM ${prefix}orders
```

**Step 3: Cube — declare the partition on the external dimension attribute**
```yaml
# cubes/orders_cube.yaml
name: ${prefix}orders_cube
node_type: cube
metrics:
  - ${prefix}total_orders
dimensions:
  - common.dimensions.time.date.dateint
  - ${prefix}orders.product_id

columns:
  - name: common.dimensions.time.date.dateint   # ← full attribute path, matches dimensions entry
    display_name: Date
    attributes:
      - primary_key
    partition:
      type: temporal
      granularity: day
      format: yyyyMMdd
```

**Result**: Queries with `include_temporal_filters=True` push `WHERE order_date >= X AND order_date <= Y` to the `orders` transform.

---

## Complete Workflow Example

**Scenario**: Add a new metric to the finance namespace.

**Step 1: Check if finance is repo-backed**

Use MCP tool (`datajunction-query`):
```
get_node_details(name="finance.total_revenue")
# Check the output for git repository info
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

**Step 3: Clone repo and check out branch**
```bash
git clone https://github.com/myorg/dj-finance.git
cd dj-finance
git checkout feature-add-churn-metric
```

**Step 4: Create metric YAML file**
```bash
cat > nodes/metrics/churn_rate.yaml <<'EOF'
name: finance.churn_rate
description: Monthly user churn rate
node_type: metric
query: |
  SELECT
    CAST(SUM(CASE WHEN churned = true THEN 1 ELSE 0 END) AS DOUBLE) /
    NULLIF(COUNT(DISTINCT user_id), 0) AS churn_rate
  FROM finance.user_activity

required_dimensions:
  - common.dimensions.date.month

metric_metadata:
  direction: lower_is_better
  unit: unitless

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

**Step 7: Test in branch namespace**
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
gh pr merge --squash
```

**Step 10: Changes sync to production** — metric now available in `finance.main` namespace.

---

## Best Practices for Repo Authoring

### Column

- ✅ Always explicitly list columns (SELECT * not supported)
- ✅ Add `display_name` for better UX in query builders
- ✅ Mark primary key columns with `primary_key` attribute
- ✅ Mark groupable columns with `dimension` attribute (transforms/facts)
- ✅ Use CAST() in query if you need specific types
- ✅ Column order in YAML should match SELECT order

### Cube

- ✅ Use `get_common_dimensions` MCP tool (`datajunction-query`) to check compatibility first
- ✅ Only use shared dimensions
- ✅ Use for frequently queried combinations
- ✅ **Always set temporal partitions on cubes** for performance
- ✅ Ensure all upstream nodes link to the same date dimension
- ✅ Match granularity to physical partition scheme in data warehouse
- ✅ Verify partition filters in generated SQL

### Workflow

1. **Always create feature branches** — never commit directly to default branch
2. **Use descriptive branch names** — `feature-add-revenue-metrics` not `fix-stuff`
3. **Write clear commit messages** — explain the "why" not just the "what"
4. **Keep PRs focused** — one logical change per PR
5. **Test in branch namespace** — validate metrics work before merging
6. **Use draft mode first** — set `mode: draft` while developing, `published` when ready
7. **Document in YAML** — use `description` fields thoroughly
8. **Assign ownership** — ideally teams rather than individuals for continuity
