---
weight: 85
title: "Metric Decomposition"
---

Metric decomposition is the process by which DataJunction breaks down complex metric expressions into
simpler, pre-aggregatable components. This enables efficient materialization to OLAP databases while
preserving the mathematical correctness of metrics when queried at different dimension granularities.

## Why Decomposition is Necessary

OLAP databases like Druid are optimized for rollup aggregations (e.g., SUM, COUNT, MIN, MAX) but
cannot directly compute complex metrics like averages or rates from pre-aggregated data. For example:

```sql
-- This metric calculates average repair price
SELECT AVG(price) FROM repair_orders
```

If we pre-aggregate this as `AVG(price)` at the daily level, we **cannot** correctly compute the
weekly average by averaging the daily averages. The mathematical property required is called
**additivity** - only additive aggregations (SUM, COUNT, MIN, MAX) can be correctly rolled up.

DataJunction solves this by decomposing metrics into their additive components.

## The Three Phases of Decomposition

Each decomposable aggregation defines three operations. Let's use `AVG(price)` as a running example:

### Phase 1: Accumulate

This phase defines how to build the raw components from source data during initial ingestion.

For `AVG(price)`, we need two components:
- `SUM(price)` -> `price_sum`
- `COUNT(price)` -> `price_count`

Each of these components are computed when data is loaded into the measures table.

### Phase 2: Merge

This phase defines how to combine pre-aggregated components when rolling up to coarser granularity. This uses associative operations that produce the same result regardless of grouping.

For our AVG components:
- `price_sum` merges via `SUM` (add the sums together)
- `price_count` merges via `SUM` (add the counts together)

**Example:** Rolling up from hourly to daily measures:

| Hour | price_sum | price_count |
|------|-----------|-------------|
| 10am | 300 | 3 |
| 11am | 500 | 5 |
| 12pm | 200 | 2 |

After merge (daily level):
| Day | price_sum | price_count |
|-----|-----------|-------------|
| Mon | 1000 | 10 |

### Phase 3: Combine

Reconstruct the final metric value from the merged components. This formula is applied at query time.

For `AVG(price)`:
```sql
SUM(price_sum) / SUM(price_count)
```

Using our example: `1000 / 10 = 100` ✓

This is mathematically equivalent to computing `AVG(price)` on the original data, but works on pre-aggregated measures.

## Decomposition by Aggregation Type

### Simple Aggregations: SUM, COUNT, MIN, MAX

These are already additive and decompose trivially:

| Metric | Accumulate | Merge | Combine |
|--------|------------|-------|---------|
| `SUM(x)` | `SUM(x)` | `SUM` | `SUM(sum_col)` |
| `COUNT(x)` | `COUNT(x)` | `SUM` | `SUM(count_col)` |
| `MIN(x)` | `MIN(x)` | `MIN` | `MIN(min_col)` |
| `MAX(x)` | `MAX(x)` | `MAX` | `MAX(max_col)` |

Note that COUNT merges as SUM (we sum up the counts).

### AVG (Average)

Average requires two components: sum and count.

| Component | Accumulate | Merge |
|-----------|------------|-------|
| `sum_col` | `SUM(x)` | `SUM` |
| `count_col` | `COUNT(x)` | `SUM` |

**Combiner:** `SUM(sum_col) / SUM(count_col)`

**Example:**
```sql
-- Original metric
SELECT AVG(price) FROM orders

-- Decomposed measures
SELECT SUM(price) AS price_sum, COUNT(price) AS price_count FROM orders

-- Combiner query (at query time)
SELECT SUM(price_sum) / SUM(price_count) FROM measures_table
```

### APPROX_COUNT_DISTINCT (HyperLogLog)

Approximate distinct counts use HyperLogLog (HLL) sketches, which are probabilistic data structures
that can be merged associatively.

| Component | Accumulate | Merge |
|-----------|------------|-------|
| `hll_col` | `hll_sketch_agg(x)` | `hll_union` |

**Combiner:** `hll_sketch_estimate(hll_union(hll_col))`

DataJunction uses Spark function names internally (`hll_sketch_agg`, `hll_union`, `hll_sketch_estimate`)
and translates to dialect-specific functions at query time:

| DJ Internal (Spark) | Druid | Trino |
|---------------------|-------|-------|
| `hll_sketch_agg` | `DS_HLL` | `approx_set` |
| `hll_union` | `DS_HLL` | `merge` |
| `hll_sketch_estimate` | `APPROX_COUNT_DISTINCT_DS_HLL` | `cardinality` |

**Example:**
```sql
-- Original metric
SELECT APPROX_COUNT_DISTINCT(user_id) FROM events

-- Decomposed measures (stored as binary HLL sketch)
SELECT hll_sketch_agg(user_id) AS user_id_hll FROM events

-- Combiner query (Spark)
SELECT hll_sketch_estimate(hll_union(user_id_hll)) FROM measures_table

-- Combiner query (Druid, after translation)
SELECT APPROX_COUNT_DISTINCT_DS_HLL(DS_HLL(user_id_hll)) FROM measures_table
```

### COUNT(DISTINCT x) - Exact Distinct Counts

Exact distinct counts **cannot** be decomposed for full pre-aggregation because there's no way to
merge distinct value sets associatively without storing all values.

DJ marks these as `LIMITED` aggregability, meaning they can only be pre-aggregated if the query
includes all columns that affect distinctness.

## Complex Metric Examples

### Rate Metrics

```sql
-- Click-through rate: clicks / impressions
SELECT SUM(clicks) / SUM(impressions) FROM ad_events
```

Decomposes to two SUM measures that merge independently and combine via division.

### Conditional Aggregations

```sql
-- Discounted order rate
SELECT CAST(SUM(IF(discount > 0, 1, 0)) AS DOUBLE) / COUNT(*) FROM orders
```

Both components (conditional sum and count) decompose normally.

### Mixed Aggregations

```sql
-- Revenue per unique user
SELECT SUM(revenue) / APPROX_COUNT_DISTINCT(user_id) FROM transactions
```

| Component | Type | Accumulate | Merge |
|-----------|------|------------|-------|
| `revenue_sum` | SUM | `SUM(revenue)` | `SUM` |
| `user_id_hll` | HLL | `hll_sketch_agg(user_id)` | `hll_union` |

**Combiner:** `SUM(revenue_sum) / hll_sketch_estimate(hll_union(user_id_hll))`

## Aggregability Classification

DJ classifies each metric's aggregability:

| Type | Description | Example |
|------|-------------|---------|
| `FULL` | Can be pre-aggregated at any dimension level | SUM, AVG, APPROX_COUNT_DISTINCT |
| `LIMITED` | Can only pre-aggregate with specific dimensions | COUNT(DISTINCT x) |
| `NONE` | Cannot be pre-aggregated | MEDIAN, percentiles, MAX_BY |

## How Materialization Uses Decomposition

When you configure a cube for materialization:

1. **DJ analyzes each metric** in the cube and extracts decomposable components
2. **A measures table** is created with columns for each component
3. **Data is ingested** using the accumulate expressions (Phase 1)
4. **At query time**, DJ generates SQL that:
   - Reads from the measures table
   - Applies merge functions for rollup (Phase 2)
   - Applies combiner expressions (Phase 3)
   - Translates functions to the target dialect

### Example: Cube with Multiple Metrics

```yaml
# Cube definition
name: sales_cube
metrics:
  - total_revenue      # SUM(amount)
  - avg_order_value    # AVG(amount)  
  - unique_customers   # APPROX_COUNT_DISTINCT(customer_id)
dimensions:
  - date
  - region
  - product_category
```

**Generated Measures Table:**

| Column | Source Metric | Accumulate |
|--------|---------------|------------|
| `amount_sum` | total_revenue, avg_order_value | `SUM(amount)` |
| `amount_count` | avg_order_value | `COUNT(amount)` |
| `customer_id_hll` | unique_customers | `hll_sketch_agg(customer_id)` |

**Query for avg_order_value by region:**

```sql
SELECT 
    region,
    SUM(amount_sum) / SUM(amount_count) AS avg_order_value
FROM sales_cube_measures
GROUP BY region
```

## Non-Decomposable Aggregations

Some aggregations cannot be decomposed for pre-aggregation:

| Function | Reason |
|----------|--------|
| `MEDIAN` | Requires all values to compute |
| `PERCENTILE` | Requires all values to compute |
| `MAX_BY(x, y)` | Cannot merge without full data |
| `MIN_BY(x, y)` | Cannot merge without full data |
| `COUNT(DISTINCT x)` | Requires all distinct values (use APPROX_COUNT_DISTINCT instead) |

Metrics using these functions will have empty measure lists and cannot benefit from cube materialization.

## SQL API: Getting Decomposed Measures

You can retrieve the decomposed measures for any metric using the API:

```bash
# Get measures for a metric
curl http://localhost:8000/nodes/default.avg_repair_price/measures/
```

Response:
```json
{
  "measures": [
    {
      "name": "price_sum_abc123",
      "expression": "price",
      "aggregation": "SUM",
      "merge": "SUM"
    },
    {
      "name": "price_count_abc123", 
      "expression": "price",
      "aggregation": "COUNT",
      "merge": "SUM"
    }
  ],
  "combiner": "SUM(price_sum_abc123) / SUM(price_count_abc123)"
}
```
