# DataJunction Builder

> **Skill**: `datajunction-builder`
> **Purpose**: Creating and managing the semantic layer - sources, dimensions, metrics, cubes
> **Requires**: `datajunction-core` (activate together)
> **Keywords**: create metric, define dimension, dimension link, build cube, publish node, metric creation

---

## Creating Source Nodes

Sources are physical tables or views in your data warehouse.

```bash
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.transactions",
  "description": "Raw transaction data from payment system",
  "catalog": "prod_catalog",
  "schema": "finance",
  "table": "transactions_table",
  "columns": [
    {"name": "transaction_id", "type": "bigint"},
    {"name": "user_id", "type": "bigint"},
    {"name": "amount_usd", "type": "decimal(10,2)"},
    {"name": "transaction_date", "type": "date"},
    {"name": "refund_flag", "type": "boolean"},
    {"name": "status", "type": "string"}
  ],
  "mode": "published"
}'
```

**Best practices:**
- Use fully qualified names: `namespace.node_name`
- Add clear descriptions
- Document all columns with correct types
- Start in `draft` mode, test, then publish

---

## Creating Dimension Nodes

Dimensions are entities with attributes (lookup tables).

```bash
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "core.users",
  "description": "User dimension with demographic attributes",
  "query": "SELECT user_id, country, signup_date, tier, segment FROM users_table",
  "primary_key": ["user_id"],
  "mode": "published"
}'
```

**Key requirements:**
- **Must** define `primary_key` - DJ uses this for joins
- Include all relevant attributes users might want to group by
- Use denormalized structure (star schema, not snowflake)

**Best practices:**
- Use `common.dimensions` namespace for shared dimensions (users, dates, regions)
- Don't recreate dimensions in multiple namespaces
- Keep attribute names clear and consistent

---

## Defining Dimension Links

### On Source Nodes (Most Common)

When the source table cleanly represents a semantic entity:

```bash
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.transactions",
  "catalog": "prod_catalog",
  "schema": "finance",
  "table": "transactions_table",
  "columns": [...],
  "dimension_links": [
    {
      "dimension": "core.users",
      "join_on": "finance.transactions.user_id = core.users.user_id",
      "join_type": "left"
    },
    {
      "dimension": "core.dates",
      "join_on": "finance.transactions.transaction_date = core.dates.date",
      "join_type": "left"
    }
  ]
}'
```

---

### On Transform Nodes (Clean Semantic Representation)

When source tables **don't** accurately represent your semantic entity:

```bash
# Source has messy data - no dimension links here
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "raw.user_events",
  "catalog": "staging",
  "schema": "raw",
  "table": "user_events_raw"
}'

# Transform cleans it up and adds dimension links
curl -X POST http://localhost:8000/nodes/transform/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "clean.user_events",
  "description": "Cleaned user events with proper types",
  "query": "
    SELECT
      event_id,
      CAST(user_id_string AS bigint) AS user_id,
      event_date,
      UPPER(country_code) AS country
    FROM raw.user_events
    WHERE user_id_string IS NOT NULL
  ",
  "dimension_links": [
    {
      "dimension": "core.users",
      "join_on": "clean.user_events.user_id = core.users.user_id",
      "join_type": "left"
    }
  ]
}'
```

**Use when**:
- Source has wrong data types
- Need to filter/clean before joining
- Business logic needed to identify entity

---

### On Dimension Nodes (Build Dimensional Graph)

Dimensions can link to other dimensions:

```bash
# Users dimension links to countries
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "core.users",
  "query": "SELECT user_id, country_code, signup_date FROM users_table",
  "primary_key": ["user_id"],
  "dimension_links": [
    {
      "dimension": "core.countries",
      "join_on": "core.users.country_code = core.countries.country_code",
      "join_type": "left"
    }
  ]
}'

# Countries dimension links to regions
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "core.countries",
  "query": "SELECT country_code, country_name, region FROM countries_table",
  "primary_key": ["country_code"],
  "dimension_links": [
    {
      "dimension": "core.regions",
      "join_on": "core.countries.region = core.regions.region",
      "join_type": "left"
    }
  ]
}'
```

**Use when**: Building interconnected dimensional hierarchies

---

### Dimension Links with Roles

When a node references the same dimension multiple times:

```bash
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "marketplace.orders",
  "catalog": "prod",
  "schema": "marketplace",
  "table": "orders",
  "dimension_links": [
    {
      "dimension": "core.users",
      "join_on": "marketplace.orders.buyer_id = core.users.user_id",
      "join_type": "left",
      "role": "buyer"
    },
    {
      "dimension": "core.users",
      "join_on": "marketplace.orders.seller_id = core.users.user_id",
      "join_type": "left",
      "role": "seller"
    }
  ]
}'
```

---

## Creating Metrics

> **Important**: Metrics select a **single expression** from a **single source, transform, or dimension node**. They cannot contain WHERE clauses - use conditional aggregations instead.

### Metric Structure

```sql
SELECT <aggregation_expression> AS <metric_name>
FROM <single_node>
```

---

### Simple Aggregation Metrics

#### COUNT

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.num_transactions",
  "description": "Total number of transactions",
  "mode": "published",
  "query": "SELECT COUNT(transaction_id) AS num_transactions FROM finance.transactions"
}'
```

#### SUM

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.total_revenue",
  "description": "Total revenue from all transactions",
  "mode": "published",
  "query": "SELECT SUM(amount_usd) AS total_revenue FROM finance.transactions",
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "dollar"
  }
}'
```

#### AVG

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.avg_transaction_value",
  "description": "Average transaction value",
  "query": "SELECT AVG(amount_usd) AS avg_transaction_value FROM finance.transactions",
  "metric_metadata": {
    "unit": "dollar"
  }
}'
```

---

### Conditional Aggregations

Since metrics cannot contain WHERE clauses, use **conditional aggregation** to filter within the SELECT:

#### Filtering with CASE WHEN

```bash
# Revenue from completed, non-refunded transactions only
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
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
  }
}'
```

#### COUNT_IF (Cleaner Syntax)

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.completed_transaction_count",
  "description": "Number of completed transactions",
  "query": "SELECT COUNT_IF(status = '\''completed'\'') AS completed_transaction_count FROM finance.transactions"
}'
```

---

### Ratio Metrics

Use **NULLIF** to avoid division by zero:

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
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
  }
}'
```

**Best practices for ratios:**
- Always use `NULLIF(denominator, 0)`
- Cast to float with `* 1.0`
- Use `unit: "proportion"` (0-1) or `unit: "percentage"` (0-100)

---

### Derived Metrics

Derived metrics reference other metrics:

```bash
# First create base metrics
curl -X POST http://localhost:8000/nodes/metric/ \
-d '{
  "name": "finance.total_revenue",
  "query": "SELECT SUM(amount_usd) AS total_revenue FROM finance.transactions"
}'

curl -X POST http://localhost:8000/nodes/metric/ \
-d '{
  "name": "finance.total_orders",
  "query": "SELECT COUNT(transaction_id) AS total_orders FROM finance.transactions"
}'

# Then create derived metric
curl -X POST http://localhost:8000/nodes/metric/ \
-d '{
  "name": "finance.average_order_value",
  "description": "Average revenue per order",
  "query": "SELECT finance.total_revenue / NULLIF(finance.total_orders, 0) AS average_order_value",
  "metric_metadata": {
    "unit": "dollar"
  }
}'
```

**Key insight**: Derived metrics automatically inherit components from base metrics for proper cube materialization.

---

### Metric Metadata

#### Required Dimensions

Some metrics only make sense when grouped by specific dimensions:

```bash
curl -X POST http://localhost:8000/nodes/metric/ \
-d '{
  "name": "finance.market_share",
  "query": "SELECT SUM(revenue) / SUM(total_market_revenue) AS market_share FROM finance.sales",
  "required_dimensions": ["finance.product.category", "core.date.quarter"]
}'
```

#### Metric Direction

| Direction | Description | Example Metrics |
|-----------|-------------|-----------------|
| `higher_is_better` | Increasing values are positive | Revenue, Conversion Rate, NPS |
| `lower_is_better` | Decreasing values are positive | Churn Rate, Error Rate, Latency |
| `neutral` | Direction doesn't indicate good/bad | Count of Users |

#### Metric Units

| Unit | Description |
|------|-------------|
| `percentage` | Values from 0 to 100 |
| `proportion` | Values from 0 to 1 |
| `dollar` | Currency (USD) |
| `second` | Time duration |
| `unitless` | No specific unit |

---

## Supported Aggregation Functions

### Fully Aggregatable (Can Pre-Aggregate in Cubes)

- `SUM` - Sum of values
- `COUNT` - Count of rows
- `COUNT_IF` - Conditional count
- `MIN` / `MAX` - Minimum / maximum
- `AVG` - Average (decomposed to SUM/COUNT)
- `APPROX_COUNT_DISTINCT` - Approximate distinct count (HyperLogLog)
- Statistical: `VAR_POP`, `STDDEV_POP`, `COVAR_POP`, `CORR`

### Non-Aggregatable (Cannot Pre-Aggregate)

- `MAX_BY` / `MIN_BY` - Require full dataset access

### Limited Aggregatable

- `COUNT(DISTINCT ...)` - Can only pre-aggregate at or below distinct grain
- For large scale, prefer `APPROX_COUNT_DISTINCT`

---

## Assembling Cubes

### ⚠️ Critical: Check Compatibility First

**All metrics in a cube MUST share ALL dimensions in the cube.**

```bash
# Step 1: Check what dimensions these metrics share
curl -X POST http://localhost:8000/metrics/common/dimensions \
-H 'Content-Type: application/json' \
-d '{
  "metrics": [
    "finance.total_revenue",
    "finance.transaction_count",
    "finance.average_order_value"
  ]
}'

# Response shows common dimensions

# Step 2: Create cube using ONLY common dimensions
curl -X POST http://localhost:8000/nodes/cube/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance.revenue_cube",
  "description": "Revenue metrics by common dimensions",
  "metrics": [
    "finance.total_revenue",
    "finance.transaction_count",
    "finance.average_order_value"
  ],
  "dimensions": [
    "core.date",
    "core.region",
    "finance.payment_method"
  ],
  "mode": "published"
}'
```

**When to use cubes:**
- Frequently queried metric combinations
- Pre-compute for performance (materialization)
- Dashboard metric sets

---

## Organizing with Collections

```bash
curl -X POST http://localhost:8000/collections/ \
-H 'Content-Type: application/json' \
-d '{
  "name": "finance_core_metrics",
  "description": "Essential finance metrics for dashboards",
  "nodes": [
    "finance.total_revenue",
    "finance.completed_revenue",
    "finance.average_order_value",
    "finance.refund_rate"
  ]
}'
```

**Use collections for:**
- Dashboard metric sets
- Team-owned metrics
- Domain-specific groupings

---

## Tagging & Metadata

```bash
# Create tag
curl -X POST http://localhost:8000/tags/ \
-d '{
  "name": "revenue",
  "description": "Revenue-related metrics"
}'

# Tag a node
curl -X POST http://localhost:8000/nodes/finance.total_revenue/tags/ \
-d '{
  "tag": "revenue"
}'
```

**Common tag patterns:**
- Domain: `revenue`, `cost`, `engagement`
- Owner: `finance-team`, `data-platform`
- Type: `kpi`, `operational`, `experimental`

---

## Validation & Testing Workflow

```bash
# 1. Validate SQL syntax
curl http://localhost:8000/nodes/finance.new_metric/validate

# 2. Test SQL generation
curl -X POST http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.new_metric"],
  "dimensions": ["core.date"],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "limit": 10,
  "engine_name": "trino"
}'

# 3. Review generated SQL

# 4. Check lineage
curl http://localhost:8000/nodes/finance.new_metric/lineage?direction=upstream

# 5. Publish
curl -X PATCH http://localhost:8000/nodes/finance.new_metric \
-d '{"mode": "published"}'
```

---

## Best Practices Summary

### Creating Nodes
- ✅ Use fully qualified names: `namespace.node_name`
- ✅ Start in `draft` mode, test, then publish
- ✅ Add clear descriptions
- ✅ Reuse existing dimensions (especially `core.*`)

### Creating Metrics
- ✅ Select single expression from single node
- ✅ Use CASE WHEN for filtering (no WHERE clauses)
- ✅ Use NULLIF for divisions
- ✅ Add metric metadata (direction, unit)
- ✅ Test with `/sql/metrics/v3` before publishing

### Creating Cubes
- ✅ Check `/metrics/common/dimensions` first
- ✅ Only use shared dimensions
- ✅ Use for frequently queried combinations
