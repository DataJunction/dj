---
weight: 80
title: "Materialization"
---

## Cube Nodes

When we attach a materialization config to a cube node (instructions
[here](../../getting-started/creating-nodes/cubes#adding-materialization-config)), we are requesting DJ to prepare
for the materialization of the cube's underlying data into an OLAP database (such as Druid). This enables
low-latency metric queries across all defined dimensions in the cube.

However, many such databases are only configured to work with simple aggregations, so DJ will break down each
metric expression into its constituent simple aggregation measures prior to materialization. These measures are
ingested into the OLAP database as separate columns and they're combined back together into the original metrics
when users request metric data.

For a detailed explanation of how this decomposition works, including supported aggregation types and
dialect translation, see [Metric Decomposition](../metric-decomposition/).

A few examples include:

<table>
<tr><th>Metric Query</th><th colspan="3">Measures Ingested</th></tr>
<tr>
<td rowspan="3">

```sql
SELECT
  AVG(price)
    AS avg_repair_price
FROM repair_order_details
```
</td>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price_count`
</td>
<td>

`count`
</td>
<td>


```sql
count(price)
```
</td>
</tr>
<tr>
<td>

`price_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price)
```
</td>
</tr>

<tr>
<td rowspan="3">

```sql
SELECT
  SUM(price)
    AS total_repair_price
FROM repair_order_details
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price)
```
</td>
</tr>





<tr>
<td rowspan="4">

```sql
SELECT
  CAST(
    SUM(
      IF(discount > 0.0,
          1, 0)
    ) AS DOUBLE
  ) / COUNT(*)
    AS discounted_orders_rate
FROM repair_order_details
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`discount_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(
  if(
    discount > 0.0,
    1, 0
  )
)
```
</td>
</tr>

<tr>
<td>

`count_star`
</td>
<td>

`count`
</td>
<td>

```sql
count(*)
```
</td>
</tr>




<tr>
<td rowspan="4">

```sql
SELECT
  SUM(price1) +
    SUM(price2)
  AS total_cost
FROM costs
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price1_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price1)
```
</td>
</tr>

<tr>
<td>

`price2_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price2)
```
</td>
</tr>




<tr>
<td rowspan="3">

```sql
SELECT
  APPROX_COUNT_DISTINCT(
    user_id
  ) AS unique_users
FROM events
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`user_id_hll`
</td>
<td>

`hll_sketch_agg`
</td>
<td>

```sql
hll_sketch_agg(user_id)
```

*Stored as binary HLL sketch*
</td>
</tr>
</table>

The combiner expression for `APPROX_COUNT_DISTINCT` uses HyperLogLog functions:
```sql
hll_sketch_estimate(hll_union(user_id_hll))
```

This is automatically translated to the appropriate dialect when querying:
- **Druid:** `APPROX_COUNT_DISTINCT_DS_HLL(DS_HLL(user_id_hll))`
- **Trino:** `cardinality(merge(user_id_hll))`

## Bring Your Own Materialization

While DJ can manage materialization end-to-end, you may want to orchestrate materialization separately. DJ supports this by generating SQL that you can execute in your own pipelines.

For incremental refresh, DJ supports **temporal partition filtering** - automatically adding partition filters to the generated SQL. Instead of scanning all historical data, queries are scoped to specific time windows using `${dj_logical_timestamp}` placeholders that your orchestration tool can substitute with actual timestamps.

### Setup

To enable temporal partition filtering for a cube, add partition configuration to your cube YAML:

```yaml
# sales_by_date.cube.yaml
description: Sales metrics by date
metrics:
  - v3.total_revenue
  - v3.order_count
dimensions:
  - v3.date.date_id
columns:
  - name: v3.date.date_id
    partition:
      type_: temporal
      format: yyyyMMdd
      granularity: day
```

### Usage

#### Basic Usage (Exact Partition)

Request measures SQL with temporal filtering enabled:

```bash
GET /sql/measures/v3/?metrics=v3.total_revenue&dimensions=v3.date.date_id&include_temporal_filters=true
```

Generated SQL:
```sql
SELECT order_date AS date_id, SUM(revenue) AS revenue_sum
FROM orders
WHERE order_date = CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS INT)
GROUP BY order_date
```

#### With Lookback Window

For scanning N days/hours back from the logical timestamp (useful for catching late-arriving data):

```bash
GET /sql/measures/v3/?metrics=v3.total_revenue&dimensions=v3.date.date_id&include_temporal_filters=true&lookback_window=7%20DAY
```

Generated SQL:
```sql
SELECT order_date AS date_id, SUM(revenue) AS revenue_sum
FROM orders
WHERE order_date BETWEEN CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP) - INTERVAL '7' DAY, 'yyyyMMdd') AS INT)
                     AND CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS INT)
GROUP BY order_date
```

### The ${dj_logical_timestamp} Placeholder

The generated SQL contains `${dj_logical_timestamp}` placeholders that must be replaced with actual timestamps before execution:

- **For DJ-managed materialization**: DJ automatically substitutes this during refresh
- **For external orchestration**: Your tool replaces this with the target partition timestamp

**Example substitution**:
```sql
-- Before substitution (what DJ returns)
WHERE order_date = CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS INT)

-- After substitution (what your orchestration runs)
WHERE order_date = CAST(DATE_FORMAT(CAST('2024-01-15T00:00:00' AS TIMESTAMP), 'yyyyMMdd') AS INT)

-- Evaluates to
WHERE order_date = 20240115
```

### When Temporal Filters Are Applied

Temporal filters are **only applied when**:
1. `include_temporal_filters=true` is set
2. The requested metrics + dimensions resolve to a cube
3. That cube has temporal partition columns configured

If no matching cube exists, the parameter is ignored and no filters are added.

