---
weight: 7
title: "Metrics"
---

Metric nodes represent an aggregation of a measure defined as a single expression in a query that selects from
a single source, transform, or dimension node.

| Attribute    | Description                                                                                 | Type   |
|--------------|---------------------------------------------------------------------------------------------|--------|
| name         | Unique name used by other nodes to select from this node                                    | string |
| display_name | A human readable name for the node                                                          | string |
| description  | A human readable description of the node                                                    | string |
| mode         | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| query        | A SQL query that selects a single expression from a single node                             | string |

## Creating Metric Nodes

{{< tabs "creating metric nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.num_repair_orders",
    "description": "Number of repair orders",
    "mode": "published",
    "query": "SELECT count(repair_order_id) as num_repair_orders FROM default.repair_orders"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJBuilder, NodeMode
dj = DJBuilder(DJ_URL)

metric = dj.create_metric(
    name="default.num_repair_orders",
    description="Number of repair orders",
    query="SELECT count(repair_order_id) FROM repair_orders",
    mode=NodeMode.PUBLISHED,  # for draft nodes, use `mode=NodeMode.DRAFT`
)
print(metric.name)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.metrics.create(
    {
        name: "default.num_repair_orders",
        description: "Number of repair orders",
        mode: "published",
        query: `
            SELECT
            count(repair_order_id) as num_repair_orders
            FROM default.repair_orders
        `
    }
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

## Supported Aggregation Functions

DJ supports a wide range of SQL aggregation functions for defining metrics. These functions are categorized by their **aggregability** - whether they can be pre-aggregated for efficient cube materialization.

### Fully Aggregatable Functions

These functions can be pre-aggregated at any grain level, enabling efficient cube materializations.

#### SUM
Calculates the sum of values.
```sql
SELECT SUM(total_repair_cost) FROM default.repair_orders
```

#### COUNT
Counts the number of rows.
```sql
SELECT COUNT(repair_order_id) FROM default.repair_orders
```

#### COUNT_IF
Counts rows that match a condition.
```sql
SELECT COUNT_IF(status = 'completed') FROM default.repair_orders
```

#### MIN
Returns the minimum value.
```sql
SELECT MIN(order_date) FROM default.repair_orders
```

#### MAX
Returns the maximum value.
```sql
SELECT MAX(total_repair_cost) FROM default.repair_orders
```

#### AVG
Calculates the average. Internally decomposed into SUM and COUNT for pre-aggregation.
```sql
SELECT AVG(total_repair_cost) FROM default.repair_orders
```

#### ANY_VALUE
Returns any value from the group.
```sql
SELECT ANY_VALUE(category) FROM default.repair_orders
```

#### APPROX_COUNT_DISTINCT
Approximate distinct count using HyperLogLog sketches. Useful for large datasets where exact distinct counts are expensive.
```sql
SELECT APPROX_COUNT_DISTINCT(customer_id) FROM default.repair_orders
```

### Statistical Functions

These functions compute statistical measures and are fully decomposable for pre-aggregation.

#### VAR_POP / VAR_SAMP / VARIANCE
Population variance, sample variance (with Bessel's correction), and VARIANCE (alias for VAR_SAMP).
```sql
SELECT VAR_POP(total_repair_cost) FROM default.repair_orders
SELECT VAR_SAMP(total_repair_cost) FROM default.repair_orders
SELECT VARIANCE(total_repair_cost) FROM default.repair_orders
```

#### STDDEV_POP / STDDEV_SAMP / STDDEV
Population standard deviation, sample standard deviation, and STDDEV (alias for STDDEV_SAMP).
```sql
SELECT STDDEV_POP(total_repair_cost) FROM default.repair_orders
SELECT STDDEV_SAMP(total_repair_cost) FROM default.repair_orders
SELECT STDDEV(total_repair_cost) FROM default.repair_orders
```

#### COVAR_POP / COVAR_SAMP
Population and sample covariance between two variables.
```sql
SELECT COVAR_POP(labor_cost, parts_cost) FROM default.repair_orders
SELECT COVAR_SAMP(labor_cost, parts_cost) FROM default.repair_orders
```

#### CORR
Pearson correlation coefficient between two variables.
```sql
SELECT CORR(labor_cost, parts_cost) FROM default.repair_orders
```

### Non-Aggregatable Functions

These functions require access to the full dataset and cannot be pre-aggregated in cubes.

#### MAX_BY / MIN_BY
Returns a value from the row with the maximum/minimum value of another column.
```sql
SELECT MAX_BY(technician_name, repair_count) FROM default.repair_orders
SELECT MIN_BY(technician_name, repair_count) FROM default.repair_orders
```

{{< alert icon="⚠️" >}}
Metrics using `MAX_BY` or `MIN_BY` cannot be included in materialized cubes because they require access to individual rows.
{{< /alert >}}

### DISTINCT Aggregations

Adding `DISTINCT` to any aggregation function makes it **limited aggregatable** - it can only be pre-aggregated at or below the grain of the distinct columns.

```sql
SELECT COUNT(DISTINCT customer_id) FROM default.repair_orders
SELECT SUM(DISTINCT order_amount) FROM default.repair_orders
```

{{< alert icon="👉" >}}
For large-scale approximate distinct counts, prefer `APPROX_COUNT_DISTINCT` which uses HyperLogLog sketches and is fully aggregatable.
{{< /alert >}}

## Derived Metrics

Derived metrics reference other metrics, allowing you to build complex calculations from simpler components. They automatically inherit the components from their base metrics, enabling proper decomposition for cube materialization.

### Simple Derived Metrics

The simplest derived metrics combine existing metrics with arithmetic operations.

```sql
-- Average order value from two base metrics
SELECT default.total_revenue / default.total_orders AS average_order_value
```

### Cross-Fact Ratio Metrics

These metrics have numerator and denominator from **different fact tables**. This is useful for metrics like conversion rates or efficiency ratios.

**Example: Order Conversion Rate**

```sql
-- Metric 1: Count of completed orders (from orders fact)
SELECT COUNT(order_id) FROM default.completed_orders

-- Metric 2: Count of page visits (from visits fact)
SELECT COUNT(visit_id) FROM default.page_visits

-- Derived metric: Conversion rate
SELECT default.completed_order_count / default.page_visit_count AS conversion_rate
```

{{< alert icon="⚠️" >}}
Cross-fact ratio metrics require that both underlying facts share common dimensions for the metric to be meaningful. DJ will join the facts on their shared dimensional grain.
{{< /alert >}}

### Period-Over-Period Metrics

Period-over-period metrics compare values across time periods, such as week-over-week or year-over-year growth. Since DJ metrics cannot contain WHERE clauses, use conditional aggregation or window functions.

**Example: Week-Over-Week Revenue Growth (using conditional aggregation)**

Create metrics that filter time periods within the SELECT clause:
```sql
-- Current week revenue
SELECT SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE, 7) THEN revenue ELSE 0 END)
FROM default.sales

-- Previous week revenue
SELECT SUM(CASE WHEN order_date >= DATE_SUB(CURRENT_DATE, 14)
                AND order_date < DATE_SUB(CURRENT_DATE, 7) THEN revenue ELSE 0 END)
FROM default.sales
```

Then create the growth metric:
```sql
-- Week-over-week growth rate
SELECT (default.current_week_revenue - default.previous_week_revenue)
       / NULLIF(default.previous_week_revenue, 0) AS wow_revenue_growth
```

**Example: Year-Over-Year Using Window Functions**

For YoY comparisons with a date dimension, use LAG:
```sql
SELECT (revenue - LAG(revenue, 1) OVER (ORDER BY year))
       / NULLIF(LAG(revenue, 1) OVER (ORDER BY year), 0) AS yoy_revenue_growth
FROM default.yearly_revenue
```

{{< alert icon="👉" >}}
Period-over-period metrics require careful handling. Use conditional aggregation (CASE WHEN) to filter time periods within the SELECT clause, or use window functions (LAG, LEAD) when working with time-series data.
{{< /alert >}}

### Trailing N-Day Metrics

Trailing metrics aggregate data over a rolling window, such as trailing 7-day or 30-day averages. These are useful for smoothing out daily fluctuations.

**Example: Trailing 7-Day Average Revenue**

```sql
-- Daily revenue metric
SELECT SUM(revenue) FROM default.sales

-- Trailing 7-day average (as a derived metric with window logic)
SELECT AVG(default.daily_revenue) OVER (
  ORDER BY date_dim
  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS trailing_7d_avg_revenue
```

**Example: Trailing 30-Day Total Orders**
```sql
SELECT SUM(default.daily_order_count) OVER (
  ORDER BY date_dim
  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
) AS trailing_30d_orders
```

{{< alert icon="👉" >}}
Trailing metrics typically require a time dimension in your cube and use SQL window functions. The window size (7 days, 30 days, etc.) determines how much historical data is included in each calculation.
{{< /alert >}}

### Combining Derived Metric Patterns

You can combine these patterns to create sophisticated metrics:

```sql
-- Trailing 7-day conversion rate (combines trailing window + cross-fact ratio)
SELECT default.trailing_7d_orders / default.trailing_7d_visits AS trailing_7d_conversion_rate

-- YoY growth of category share (combines period-over-period + share of total)
SELECT (default.current_year_category_share - default.previous_year_category_share)
AS category_share_yoy_change
```

### Share of Total Metrics

A share of total divides a value for one slice by the total across every slice. The numerator follows whatever the query groups by; the denominator ignores it.

`fixed_grain` declares the grain an aggregate is computed at, independently of the grain the query asks for. Setting it to `[]` means the global grain, so the metric is computed once over the whole result and broadcast to every row.

```yaml
name: default.total_revenue_all_categories
query: SELECT SUM(revenue) FROM default.sales
fixed_grain: []
```

Dividing a normal metric by that one gives the share, and the ratio works at any grain the query chooses:

```sql
-- Each category's share of revenue across all categories
SELECT default.total_revenue / default.total_revenue_all_categories AS revenue_share
```

Grouped by category, the numerator splits per category while the denominator stays whole on every row, so the shares sum to 1.

DJ compiles a fixed grain into a second aggregation layered over the first:

```sql
SELECT
  category,
  SUM(SUM(revenue_sum)) OVER () AS total_revenue_all_categories,
  SUM(revenue_sum) AS total_revenue
FROM sales_0
GROUP BY category
```

#### Fixing the grain to specific dimensions

`fixed_grain` also accepts a list of dimensions, which becomes the `PARTITION BY` set. The metric is then held constant within each combination of those dimensions rather than across the whole result — useful for a share of a subtotal, such as each category's share of revenue *within its region*.

```yaml
name: default.total_revenue_by_region
query: SELECT SUM(revenue) FROM default.sales
fixed_grain: [default.store.region]
```

Omitting `fixed_grain` entirely is different from setting it to `[]`: the first means the query grain, the second means the global grain.

{{< alert icon="⚠️" >}}
**A fixed grain broadcasts an aggregate; it does not deduplicate one.** It aggregates twice — once at the query's grain, then again across the partition — so if the parent holds more than one row per entity, the first pass already overcounts and the second faithfully adds up the overcount.

Say a fact has one row per account and device, and `eligible` describes the account rather than the device:

| account | device | eligible |
|---------|--------|----------|
| A       | TV     | 1        |
| A       | phone  | 1        |
| B       | TV     | 1        |

Sliced by device, the first pass gives TV 2 and phone 1. The partition then sums those to 3, though only two accounts exist. Apply a fixed grain on a parent where each entity you are measuring appears once.
{{< /alert >}}

{{< alert icon="👉" >}}
A measure that has to ignore a dimension the fact repeats across — the case above — is not expressible yet. Track progress at [GitHub Issue #2245](https://github.com/DataJunction/dj/issues/2245).
{{< /alert >}}

## Conditional Aggregations

You can use `CASE WHEN` expressions inside aggregation functions to create metrics that only aggregate values meeting certain conditions.

**Example: Revenue from Completed Orders Only**
```sql
SELECT SUM(CASE WHEN status = 'completed' THEN revenue ELSE 0 END)
FROM default.orders
```

**Example: Completion Rate**
```sql
SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*)
FROM default.orders
```

**Example: Average for High-Value Orders**
```sql
SELECT AVG(CASE WHEN order_value > 100 THEN order_value ELSE NULL END)
FROM default.orders
```

{{< alert icon="👉" >}}
For simple conditional counts, prefer `COUNT_IF` which is more readable: `COUNT_IF(status = 'completed')`
{{< /alert >}}

## Semi-Additive Metrics

Some measures can be added up along one dimension but not another. A daily balance, a headcount, or a subscriber snapshot can be summed across accounts or regions, but summing it across dates is meaningless: the same balance counted on Monday, Tuesday and Wednesday returns three times the money that exists.

Measures like these are **semi-additive**, and `reaggregate` declares which dimension they cannot be summed along and what to do instead.

```yaml
name: default.account_balance
query: SELECT SUM(balance) FROM default.daily_account_snapshot
reaggregate:
  rules:
    - dimension: default.date.dateint
      fn: last_value
```

With date in the output grain, a query returns one balance per date, the same as any other metric. Over a date range *without* date in the grain, DJ pulls the date into the query's internal grain anyway, aggregates there, then collapses the date axis with the declared function rather than summing it:

```sql
WITH snapshot_0 AS (
  SELECT dateint, SUM(balance) AS balance_sum
  FROM default.daily_account_snapshot
  GROUP BY dateint
)
SELECT MAX_BY(balance_sum, dateint) AS account_balance
FROM snapshot_0
```

The result is the latest date's balance, not the sum of every date's. Slicing by another dimension returns each slice's own latest value, and those sum to the unsliced total.

### Collapse functions

A rule names the dimension to collapse and the function to collapse it with. Four functions can collapse a dimension:

| Function | Collapses to |
|----------|--------------|
| `last_value` | The value at the dimension's maximum, for end-of-period snapshots |
| `first_value` | The value at the dimension's minimum, for start-of-period snapshots |
| `max` | The largest value across the dimension, for peaks |
| `min` | The smallest value across the dimension, for troughs |

### Choosing between a rule and a required dimension

Both keep a snapshot from being summed along a dimension, but they differ in what a query that omits it gets back.

A required dimension rejects the query. A `reaggregate` rule answers it, returning the collapsed value. Use a required dimension where no single value across the dimension is meaningful, and a rule where one is.

{{< alert icon="⚠️" >}}
A semi-additive metric cannot be queried alongside a plain additive metric from the same parent. The two need different internal grains, so DJ splits them into separate grain groups and rejects the query rather than risk fanning one out before the final aggregation. Query them separately.
{{< /alert >}}

{{< alert icon="👉" >}}
A rule collapses a dimension; it does not build a window. Declaring `last_value` over date makes a snapshot safe to query across a date range, but it will not turn daily flags into a distinct count over a trailing window, which is a union across days rather than a single day's value.
{{< /alert >}}

## Metric Metadata

Beyond the SQL query, metrics support additional metadata to improve discoverability and usability.

### Required Dimensions

Some metrics only make sense when grouped by specific dimensions. You can specify required dimensions that must be included when querying the metric.

```yaml
name: default.market_share
query: SELECT SUM(revenue) / SUM(total_market_revenue) FROM default.sales
required_dimensions: [default.product.category, default.date.quarter]
```

When a metric has required dimensions, DJ will enforce that these dimensions are included in any query using this metric.

### Metric Direction

Indicate whether higher or lower values are "better" for the metric. This helps consumers understand how to interpret changes.

| Direction | Description | Example Metrics |
|-----------|-------------|-----------------|
| `higher_is_better` | Increasing values are positive | Revenue, Conversion Rate, NPS |
| `lower_is_better` | Decreasing values are positive | Churn Rate, Error Rate, Latency |
| `neutral` | Direction doesn't indicate good/bad | Count of Users, Average Order Size |

```yaml
name: default.customer_churn_rate
query: SELECT ...
direction: lower_is_better
```

### Metric Units

Specify the unit of measure for the metric to help consumers interpret values correctly.

| Unit | Abbreviation | Description |
|------|--------------|-------------|
| `percentage` | % | Values from 0 to 100 |
| `proportion` | | Values from 0 to 1 |
| `dollar` | $ | Currency (USD) |
| `second` | s | Time duration |
| `minute` | m | Time duration |
| `hour` | h | Time duration |
| `day` | d | Time duration |
| `week` | w | Time duration |
| `unitless` | | No specific unit |

```yaml
name: default.avg_response_time
query: SELECT AVG(response_time_ms) / 1000 FROM default.requests
unit: second
```
