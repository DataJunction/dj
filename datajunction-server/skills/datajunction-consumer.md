---
name: datajunction-consumer
description: |
  Activate this skill when querying or discovering DataJunction metrics.
  Covers finding available metrics, checking dimensions, generating SQL, and running queries.
  Keywords:
  - query metric, query metrics
  - generate SQL, SQL generation
  - available dimensions, common dimensions
  - run query, execute query
  - list metrics, find metrics
  - DJ, DataJunction
user-invocable: false
---

# DataJunction Consumer

## Discovery Workflow

### 1. Find Available Metrics

```bash
# List all metrics
curl http://localhost:8000/nodes?node_type=metric

# Filter by namespace
curl http://localhost:8000/nodes?node_type=metric&namespace=finance

# Search by name
curl http://localhost:8000/nodes?name_contains=revenue
```

---

### 2. Understand a Metric

```bash
# Get full details
curl http://localhost:8000/nodes/finance.total_revenue

# Response includes:
# - Description
# - SQL definition
# - Available dimensions (via dimension links)
# - Metric metadata (unit, direction, required dimensions)
# - Upstream dependencies
# - Tags and collections
```

---

### 3. Check Available Dimensions

```bash
# Get node details - includes all available dimensions
curl http://localhost:8000/nodes/finance.total_revenue

# Look for "dimensions" array in response
# These are ALL dimensions you can group by
```

---

### 4. Check Common Dimensions Across Metrics

```bash
# Find dimensions available across ALL specified metrics
curl -X GET http://localhost:8000/metrics/common/dimensions \
-H 'Content-Type: application/json' \
-d '{
  "metrics": [
    "finance.total_revenue",
    "growth.daily_active_users"
  ]
}'

# Returns only dimensions shared by BOTH metrics
```

**Use this before querying multiple metrics together!**

#### Roles

In some cases, dimensions may be available with different **roles**, including with various role-based paths.
For example:
  demo.main.market.active_market[primary->active_market]
  demo.main.user.user_id[reader]
  demo.main.user.user_id[writer]

These roles take on semantically different meanings and it is important to ask for a dimension with the right role in the context of a given metric.

---

## Building Queries with V3 Endpoints

### ⚠️ Important: Use V3 Endpoints

**Always use `/sql/metrics/v3` and `/sql/measures/v3`** - the old endpoints are deprecated.

---

### Basic Metric Query

```bash
curl -X GET http://localhost:8000/sql/metrics/v3 \
-H 'Content-Type: application/json' \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "orderby": ["core.date.date"],
  "limit": 100,
  "engine_name": "trino",
  "engine_version": "latest"
}'

# Returns:
# - Generated SQL for the specified engine
# - Output columns with types
# - Upstream tables used
```

---

### Multi-Metric Query

```bash
curl -X GET http://localhost:8000/sql/metrics/v3 \
-H 'Content-Type: application/json' \
-d '{
  "metrics": [
    "finance.total_revenue",
    "finance.transaction_count",
    "finance.average_order_value"
  ],
  "dimensions": [
    "core.date.date",
    "core.region.region_name"
  ],
  "filters": [
    "core.date.date >= '\''2024-01-01'\''",
    "core.region.region_name IN ('\''US'\'', '\''EU'\'')"
  ],
  "engine_name": "spark"
}'
```

**Best practice**: Check common dimensions first:
```bash
curl -X POST /metrics/common/dimensions -d '{"metrics": [...]}'
```

---

### Complex Filters

```bash
curl -X GET http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "filters": [
    "core.date.date >= '\''2024-01-01'\''",
    "core.date.date < '\''2024-04-01'\''",
    "finance.transactions.amount_usd > 100",
    "users.tier = '\''premium'\''"
  ],
  "engine_name": "trino"
}'
```

**Filter tips:**
- Reference dimension attributes: `users.country = '\''US'\''`
- Use SQL functions: `DATE_TRUNC('\''month'\'', core.date.date)`
- Multiple conditions are AND-ed together
- Use `IN` for multiple values: `region IN ('\''US'\'', '\''EU'\'', '\''APAC'\'')`

---

### Ordering & Limiting

```bash
curl -X GET http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date", "core.region.region_name"],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "orderby": [
    "core.date.date DESC",
    "finance.total_revenue DESC"
  ],
  "limit": 1000,
  "engine_name": "trino"
}'
```

**Ordering tips:**
- Order by dimensions: `"core.date.date ASC"`
- Order by metrics: `"finance.total_revenue DESC"`
- Multiple order clauses applied in sequence
- Add `DESC` for descending, default is ascending

---

### Engine-Specific SQL

Generate SQL for different engines from the same semantic query:

```bash
# Trino (interactive queries)
curl -X GET http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "engine_name": "trino"
}'

# Spark (large batch queries)
curl -X GET http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "engine_name": "spark"
}'

# DuckDB (local analytics)
curl -X GET http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "engine_name": "duckdb"
}'
```

**DJ handles dialect differences automatically** - same semantic query, different SQL output.

---

### Pre-Aggregation SQL (for Materialization)

Use `/sql/measures/v3` to generate SQL for pre-aggregated measures:

```bash
curl -X GET http://localhost:8000/sql/measures/v3 \
-H 'Content-Type: application/json' \
-d '{
  "metrics": ["finance.total_revenue", "finance.transaction_count"],
  "dimensions": [
    "core.date.date",
    "core.region.region_name",
    "finance.payment_method.method_name"
  ],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "engine_name": "spark"
}'
```

**When to use `/sql/measures/v3`:**
- Creating materialized tables/cubes
- Pre-computing at fine grain for flexibility
- Caching expensive aggregations

**Difference from `/sql/metrics/v3`:**
- `/sql/metrics/v3`: Final aggregated metrics (for querying)
- `/sql/measures/v3`: Pre-aggregated measures (for materialization)

---

## Using Client Libraries

### Python Client

```python
from datajunction import DJBuilder

dj = DJBuilder("https://dj.company.com")

# Generate SQL
result = dj.sql(
    metrics=["finance.total_revenue", "finance.transaction_count"],
    dimensions=["core.date.date", "core.region.region_name"],
    filters=["core.date.date >= '2024-01-01'"],
    engine="trino"
)

# Get generated SQL
print(result.sql)

# Execute and get pandas DataFrame
df = result.execute()
```

### JavaScript Client

```javascript
const dj = new DJClient("https://dj.company.com");

// Generate SQL
const result = await dj.sql({
  metrics: ["finance.total_revenue"],
  dimensions: ["core.date.date"],
  filters: ["core.date.date >= '2024-01-01'"],
  engine: "trino"
});

console.log(result.sql);
```

---

## Lineage & Impact Analysis

### Upstream Lineage (What data sources does this use?)

```bash
curl http://localhost:8000/nodes/finance.total_revenue/lineage?direction=upstream

# Shows:
# - Source tables
# - Transform nodes
# - Dimension nodes
# - Complete dependency chain
```

**Use when:**
- Understanding data sources for a metric
- Debugging data quality issues
- Documenting data lineage

---

### Downstream Lineage (What will break if I change this?)

```bash
curl http://localhost:8000/nodes/finance.transactions/lineage?direction=downstream

# Shows:
# - Metrics that depend on this node
# - Cubes that include these metrics
# - Transforms that reference this node
```

**Use when:**
- Planning changes to nodes
- Impact analysis before modifications
- Understanding dependencies

---

## GraphQL API

DJ also exposes a GraphQL API at `/graphql`:

### Query Nodes

```graphql
query SearchMetrics {
  nodes(nodeType: METRIC, namespace: "finance") {
    name
    description
    status
    dimensions {
      name
      type
    }
  }
}
```

### Get Common Dimensions

```graphql
query CommonDims {
  commonDimensions(
    metrics: ["finance.total_revenue", "growth.daily_active_users"]
  ) {
    name
    type
  }
}
```

---

## Performance Optimization

### Query Planning

```bash
# Get query execution plan
curl -X POST http://localhost:8000/sql/metrics/v3 \
-d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["core.date.date"],
  "explain": true
}'

# Returns execution plan showing:
# - Join order
# - Filter pushdown
# - Estimated cost
```

### Optimization Tips

1. **Use specific date ranges**
   ```bash
   # ❌ Don't query all history
   "filters": []

   # ✅ Do add date filters
   "filters": ["core.date.date >= '2024-01-01'"]
   ```

2. **Limit dimensions**
   ```bash
   # ❌ Don't add unnecessary dimensions (more joins = slower)
   "dimensions": ["dim1", "dim2", "dim3", "dim4", "dim5"]

   # ✅ Do use only what you need
   "dimensions": ["core.date.date", "core.region.region_name"]
   ```

3. **Check for materialized cubes**
   ```bash
   # Query may use pre-computed cube automatically
   curl /sql/metrics/v3 -d '{...}'
   ```

4. **Choose right engine**
   - **Trino**: Interactive queries (< 10GB data)
   - **Spark**: Large batch queries (> 10GB data)
   - **DuckDB**: Local analytics

5. **Add filters early**
   - Filters push down to source tables
   - Reduces data scanned

---

## Common Query Patterns

### Time Series Analysis

```bash
curl -X POST /sql/metrics/v3 -d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["common.dimensions.time.date"],
  "filters": [
    "core.date.date >= '\''2024-01-01'\''",
    "core.date.date < '\''2024-04-01'\''"
  ],
  "orderby": ["common.dimensions.time.date ASC"],
  "engine_name": "trino"
}'
```

### Geographic Breakdown

```bash
curl -X GET /sql/metrics/v3 -d '{
  "metrics": ["finance.total_revenue", "finance.transaction_count"],
  "dimensions": ["core.region.region_name", "core.country.country_name"],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "orderby": ["finance.total_revenue DESC"],
  "limit": 100,
  "engine_name": "trino"
}'
```

### Cohort Analysis

```bash
curl -X GET /sql/metrics/v3 -d '{
  "metrics": ["growth.retention_rate"],
  "dimensions": ["users.signup_month", "core.date.month"],
  "filters": ["users.signup_month >= '\''2024-01'\''"],
  "engine_name": "trino"
}'
```

### Top N Analysis

```bash
curl -X GET /sql/metrics/v3 -d '{
  "metrics": ["finance.total_revenue"],
  "dimensions": ["products.product_name"],
  "filters": ["core.date.date >= '\''2024-01-01'\''"],
  "orderby": ["finance.total_revenue DESC"],
  "limit": 10,
  "engine_name": "trino"
}'
```

---

## Troubleshooting Queries

### "No path found between nodes"

**Problem**: DJ cannot find a join path between the metric's source and requested dimension.

**Solutions:**
1. Check dimension links are defined on the source/transform
2. Use `/metrics/common/dimensions` to verify the dimension is available
3. Check lineage to understand the dimensional graph

---

### "Dimension not available on metric"

**Problem**: Requested dimension is not accessible from this metric.

**Solutions:**
1. Get available dimensions: `GET /nodes/{metric_name}`
2. Check dimension links on the metric's source node
3. May need to add dimension link to source/transform

---

### Query too slow

**Problem**: Generated SQL takes too long to execute.

**Solutions:**
1. Add date range filters
2. Reduce number of dimensions
3. Check if materialized cube exists
4. Use appropriate engine (Trino vs Spark)
5. Consider materializing the cube

---

### "Required dimension missing"

**Problem**: Metric has required dimensions that must be included.

**Solution:**
```bash
# Get metric details to see required dimensions
curl /nodes/finance.market_share

# Include all required dimensions in query
curl -X POST /sql/metrics/v3 -d '{
  "metrics": ["finance.market_share"],
  "dimensions": [
    "finance.product.category",  # Required
    "core.date.quarter"          # Required
  ]
}'
```

---

## Best Practices Summary

### Discovery
- ✅ Use `/nodes?node_type=metric` to find metrics
- ✅ Check node details to understand definitions
- ✅ Use `/metrics/common/dimensions` before querying multiple metrics

### Querying
- ✅ Always use V3 endpoints: `/sql/metrics/v3` and `/sql/measures/v3`
- ✅ Add specific date range filters
- ✅ Check available dimensions first
- ✅ Choose appropriate engine for workload
- ✅ Use `limit` for exploratory queries

### Optimization
- ✅ Minimize dimensions (fewer joins)
- ✅ Add filters early (pushdown to sources)
- ✅ Check for materialized cubes
- ✅ Use lineage to understand query complexity
