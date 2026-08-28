---
name: datajunction-repo
description: |
  Activate this skill when authoring DataJunction (DJ) nodes via YAML files
  in a git repository — the repo-backed workflow. Covers YAML schemas per
  node type, branch-based development, temporal partitions and
  materialization on cubes, registering pre-aggregations / aggregate
  awareness, and the full PR-driven deployment flow.
  Keywords:
  - YAML nodes, YAML definitions
  - repo-backed namespace, repo-backed workflow
  - git workflow, branch development, feature branch
  - cube YAML, metric YAML, dimension YAML, transform YAML
  - add a metric YAML file, add a dimension YAML file, add a cube YAML file
  - temporal partition, partition pushdown
  - materialize a cube, cube materialization, materialization block
  - materialization: none, coverage, lookback_window, backfill
  - pre-commit, push.sh
  - pre-aggregation, pre-agg, preagg, kind: preagg
  - aggregate awareness, aggregate navigation, query routing
  - external pre-aggregation, externally-built aggregate, registered aggregate
  - multiple tables for a metric, fact table and agg table, fact/agg hierarchy
  - summary table, rollup table, agg table, materialized aggregate
  - metrics map, dimensions map, column binding, valid_through_ts, availability
  - freshness, join back, retained key
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

## Materializing a Cube

A cube can be pre-computed on a schedule into a Druid datasource, so dashboards read
a table instead of re-running the metrics query every time. Declare that in the cube's
own YAML. The block is reviewed in the same PR as the cube it belongs to, `dj pull`
writes it back out, and it is the only route that reaches coverage backfills, more
than one strategy, and the Druid, Spark and platform settings. The API exists for the
UI and for one-off operations against a cube that already has a materialization.

### The `materialization:` block

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
  - name: common.dimensions.time.date.dateint
    attributes:
      - primary_key
    partition:
      type: temporal
      granularity: day
      format: yyyyMMdd

materialization:
  schedule: "@daily"
  strategy: incremental_time
  lookback_window: 3 DAYS
```

- **`schedule`** — required; a cron expression such as `0 6 * * *`, or a shorthand
  like `@daily`.
- **`strategy`** — `incremental_time` (the default) rebuilds the last
  `lookback_window` of partitions on each run; `full` rebuilds the whole datasource.
  Those two are the only strategies a cube takes.
- **`lookback_window`** — how far back an incremental run reaches, `1 DAY` by
  default. It carries no meaning under `full`, and DJ normalizes it away there, so a
  leftover value neither errors nor churns the live workflow.

An `incremental_time` build needs a temporal partition to increment over, and DJ reads
that off the cube's own `columns:` rather than off the materialization block — which
is what lets a cube carrying the same date dimension in two roles say which role
partitions it. Declaring `incremental_time` on a cube with no temporal partition fails
the deploy up front rather than later in the build.

### Declaring more than one

`materialization:` also takes a list, which a cube legitimately needs: an
`incremental_time` build for freshness beside a periodic `full` rebuild that corrects
late-arriving data, out-of-order events and dimension backfills.

```yaml
materialization:
  - schedule: "@daily"
    strategy: incremental_time
    lookback_window: 3 DAYS
  - schedule: "0 4 * * 0"
    strategy: full
```

`strategy` is what tells two entries apart, so each may appear at most once and a
second entry sharing one is rejected. An empty list is rejected too, since it cannot
be told apart from the teardown sentinel below. The scalar form stays valid and means
exactly what it always did — nothing has to be rewritten as a one-element list.

### Coverage

`coverage` says how much history the cube should serve, and DJ launches a backfill to
close the gap between that and what the datasource holds. Declare a fixed span or a
rolling window, never both:

```yaml
coverage: {from: 2024-01-01}                 # ongoing
coverage: {from: 2024-01-01, to: 2024-06-30}
coverage: {window: 400 DAYS}                 # rolling
```

Both endpoints are inclusive, `to` needs a `from` to go with it, and an ongoing span
ends yesterday — the last full day. A rolling window is counted in days and weeks
only, so `12 MONTHS` parses but resolves to nothing and the deploy warns instead of
backfilling.

What the backfill actually covers depends on what is already there. A datasource this
deploy minted — a materialization the cube did not have, or a rebuilt cube version —
is empty, so the whole span goes in. A datasource the cube already has gets only the
**leading** gap: availability is a bounding box, min of mins to max of maxes, so the
one thing DJ can see is that the cube starts later than the coverage asks. A hole in
the middle of the span is invisible from here and no coverage change will fill it, and
the trailing edge is the schedule's job. DJ records each span it launches and never
asks for it twice, since availability does not move until the backfill lands.

A branch deploy asks for no backfill at all and says so in its report — it previews
what the push would give its author, and a preview is not worth hundreds of partition
runs.

### Druid, Spark and platform settings

- **`druid`** — deep-merged into the ingestion spec DJ generates, so state only what
  differs from it.
- **`spark`** — conf for the stages the build runs, in three tiers that merge over one
  another. A measures job scans one parent's fact table while the combine job reads
  what those jobs already aggregated, so one conf rarely sizes both.
- **`platform`** — free-form, carried to the query service verbatim. DJ reads no key
  out of it, so nothing put here changes what DJ builds.

```yaml
spark:
  default: {spark.executor.memory: 8g}
  combiner: {spark.sql.shuffle.partitions: "200"}
  measures:
    ${prefix}orders: {spark.executor.memory: 32g}
```

`measures` is keyed by parent node name, with `${prefix}` resolved as everywhere else
in the spec, and a parent with measures jobs at several grains gets the same conf on
all of them.

### Removing one — deliberately, or by accident

Two distinctions here are destructive to get wrong.

**`materialization: none` tears down whatever the cube has materialized; omitting the
key leaves it alone.** Removal is spelled as a value rather than inferred from an
absent key because a spec round-tripped through serialization emits every optional
field explicitly, and could not otherwise tell "not managed in YAML" from "should not
be materialized". A cube that declares nothing but is materialized keeps its workflow
running and earns a warning naming both ways out — `dj pull` to adopt it into YAML, or
`materialization: none` to remove it. The sentinel takes down whatever is active,
including dialects a block cannot describe, such as a cube planner materialization.

**With a list, any active materialization no declared block built is deactivated.**
The blocks describe *the* materializations for their cube: they all write one
datasource and a full rebuild replaces it wholesale, so a competing writer cannot be
left running. Deleting an entry therefore stops that entry's workflow, exactly as
`none` would for the cube as a whole. Read the YAML that is there before editing
someone else's cube. The one row left alone is a cube planner's, which DJ cannot
rebuild from a block and so will not supersede.

### What the block does not do

`retention` is accepted and parsed, and then never reaches the config the reconciler
builds — so a declared value is silently ignored and the datasource keeps the 400-day
default. Don't offer it as a knob until it is threaded through.

### When to reach for the API instead

Backfilling a cube that is already materialized is the honest API case. Creating a
materialization usually is not: one configured through the API is invisible to review,
and the next deploy of a cube that declares its own block supersedes it.

```
POST   /nodes/{node_name}/materialization/                              upsert one
GET    /nodes/{node_name}/materializations/                             list them
DELETE /nodes/{node_name}/materializations/?materialization_name=NAME   deactivate one
POST   /nodes/{node_name}/materializations/{materialization_name}/backfill
GET    /cubes/{name}/materialization                                    preview the config
POST   /cubes/{name}/materialize                                        cube planner path
POST   /cubes/{name}/backfill                                           cube planner backfill
DELETE /cubes/{name}/materialize
```

Note the trailing slashes: they are on the `/nodes/` routes and not on the rest. The
generic upsert takes a body discriminated on `job`; for a cube that is
`job: druid_cube` plus the same flat fields the YAML block uses. `DELETE` takes
`materialization_name` as a query parameter, not a body. The node backfill takes a
JSON array of `{column_name, range}` partition specs.

**The `/cubes/` routes only ever address a row named `druid_cube_v3`**, and
only `POST /cubes/{name}/materialize` ever writes one. A YAML block — like the generic
upsert — produces a row named after job, strategy and partition instead, so
`POST /cubes/{name}/backfill` answers "has no materialization" for a cube materialized
from YAML, and `DELETE /cubes/{name}/materialize` 404s on it. Backfill a
declared cube through the `/nodes/` backfill route above, taking the name from
`GET /nodes/{node_name}/materializations/` — or widen `coverage`
and let the next deploy launch it.

---

## Pre-Aggregations / Aggregate Awareness

One concept, many names — all of these mean the same thing: **aggregate awareness**,
**aggregate navigation**, **query routing**, **pre-aggregations** / **pre-aggs**,
**external** or **registered aggregates**, **multiple tables for a metric** (a raw
fact table plus coarser aggregates), **summary tables**, **rollup tables**, **agg
tables**, **materialized aggregates**, **fact/agg hierarchy**, **last-mile** and
**intermediate aggregates**.

A metric is defined once against its fact table; the same numbers often exist
pre-summed in coarser tables. Register those tables and DJ picks per query which to
read, falling back to the fact table when no aggregate can answer correctly.

**The rules live in the docs, not here** — see
[Query Routing & Aggregate Awareness](https://datajunction.io/docs/0.1.0/dj-concepts/query-routing-aggregate-awareness/)
for the `kind: preagg` schema (`metrics` and `dimensions` are maps from each
reference to the physical column holding it), what makes a metric mappable,
role-qualified dimension references, freshness reporting, and the current limitations. Read it before registering anything; the rules that decide
whether your table actually gets used are not guessable.

### The one thing to get right before you author metrics

A pre-agg's `metrics` map binds a **metric to one column**, so a metric decomposing
into more than one component can never be bound to an aggregate — and its components are
auto-named with a hash suffix that YAML cannot address. `SUM(revenue) /
COUNT(DISTINCT view_id)` as a single node is unmappable forever; fixing it means
refactoring a node other teams may already query.

So **give every aggregation primitive its own metric node** and compose ratios as
derived metrics referencing them. Free if done from the start, expensive to retrofit,
worth doing even before an aggregate table exists.

### Repo-specific traps

**Declare partitions in YAML, never through the API.** A temporal partition on the
parent's date column is what lets DJ reason about when an aggregate's data ends:

```yaml
columns:
  - name: activity_date
    type: int
    partition:
      type: temporal
      granularity: day
      format: yyyyMMdd
```

`POST /nodes/{node}/columns/{col}/partition/` works, but the next deploy recreates
the node from YAML and reverts it — and without a temporal partition column,
freshness checks have no axis and silently do nothing.

**A parent-node edit strands its aggregates.** Registrations are keyed by node
revision, so even a description-only change re-registers them at a new revision. A
repo deploy re-creates the binding, but **availability is not restored** — until the
pipeline re-reports it, the aggregate is unused and every query silently reverts to
the fact table.

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
