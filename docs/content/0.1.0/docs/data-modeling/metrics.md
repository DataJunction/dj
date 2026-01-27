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

### Share of Total Metrics (Limited Support)

Share of total metrics calculate a proportion where the numerator is grouped by a dimension while the denominator represents the ungrouped total. This pattern requires special handling because DJ computes all metrics at the same grain.

{{< alert icon="⚠️" >}}
DJ doesn't automatically compute the numerator and denominator at different grains from the same fact. The workarounds below may be needed.
{{< /alert >}}

{{< alert icon="👉" >}}
Native support for ratio metrics at different grains is planned. Track progress at [GitHub Issue #1695](https://github.com/DataJunction/dj/issues/1695).
{{< /alert >}}

**Option 1: Conditional Aggregation**

Use a single metric with conditional aggregation:
```sql
-- Category share using a parameter for the category filter
SELECT SUM(CASE WHEN category = ${category_filter} THEN revenue ELSE 0 END) / SUM(revenue)
FROM default.sales
```

**Option 2: Application Layer**

Create separate metrics and compute the ratio at the application layer:
```sql
-- Metric 1: Revenue (will be grouped by category when queried)
SELECT SUM(revenue) FROM default.sales

-- Metric 2: Total revenue (query separately without category dimension)
SELECT SUM(revenue) FROM default.sales
```

Then divide in your application code after querying each metric at its appropriate grain.

**Option 3: Pre-computed Totals**

If you have a transform node that pre-computes totals, you can join against it:
```sql
-- Assuming default.sales_with_totals has both line-level and total columns
SELECT SUM(revenue) / MAX(total_revenue) FROM default.sales_with_totals
```

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

## Metric Metadata

Beyond the SQL query, metrics support additional metadata to improve discoverability and usability.

### Required Dimensions

Some metrics only make sense when grouped by specific dimensions. You can specify required dimensions that must be included when querying the metric.

```json
{
  "name": "default.market_share",
  "query": "SELECT SUM(revenue) / SUM(total_market_revenue) FROM default.sales",
  "required_dimensions": ["default.product.category", "default.date.quarter"]
}
```

When a metric has required dimensions, DJ will enforce that these dimensions are included in any query using this metric.

### Metric Direction

Indicate whether higher or lower values are "better" for the metric. This helps consumers understand how to interpret changes.

| Direction | Description | Example Metrics |
|-----------|-------------|-----------------|
| `higher_is_better` | Increasing values are positive | Revenue, Conversion Rate, NPS |
| `lower_is_better` | Decreasing values are positive | Churn Rate, Error Rate, Latency |
| `neutral` | Direction doesn't indicate good/bad | Count of Users, Average Order Size |

```json
{
  "name": "default.customer_churn_rate",
  "query": "SELECT ...",
  "direction": "lower_is_better"
}
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

```json
{
  "name": "default.avg_response_time",
  "query": "SELECT AVG(response_time_ms) / 1000 FROM default.requests",
  "unit": "second"
}
```
