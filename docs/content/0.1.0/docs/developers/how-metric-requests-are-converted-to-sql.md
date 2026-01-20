---
weight: 50
title: How Metric Requests are Converted to SQL
---

# How Metric Requests are Converted to SQL

This document explains how DataJunction converts metric requests into SQL queries. Understanding this flow is essential for:
- Debugging generated SQL
- Optimizing metric definitions
- Contributing to the SQL generation code

For background on metric decomposition (the theory behind breaking metrics into pre-aggregatable components), see [Metric Decomposition]({{< ref "/docs/dj-concepts/metric-decomposition" >}}).

## Overview Architecture

```
User Request (metrics + dimensions + filters)
                    |
                    v
+-------------------------------------------------------------+
|                    DECOMPOSITION PHASE                       |
|  - Load metric nodes and their dependencies                  |
|  - Extract components from base metrics                      |
|  - Build parent_map for derived metric relationships         |
|  - Identify window metrics (LAG/LEAD/etc.)                   |
+-------------------------------------------------------------+
                    |
                    v
+-------------------------------------------------------------+
|                    MEASURES PHASE (measures.py)              |
|  - Group metrics by parent fact table -> MetricGroups        |
|  - Analyze aggregability (FULL, LIMITED, NONE)               |
|  - Build GrainGroupSQLs (pre-aggregated CTEs)                |
|  - Build window metric grain groups (coarser grain CTEs)     |
+-------------------------------------------------------------+
                    |
                    v
+-------------------------------------------------------------+
|                    METRICS PHASE (metrics.py)                |
|  - Combine grain groups via FULL OUTER JOIN -> base_metrics  |
|  - Apply combiner expressions (e.g., SUM(a)/SUM(b))          |
|  - Build window CTEs (reaggregate + apply LAG/LEAD)          |
|  - Build final SELECT with all metrics                       |
+-------------------------------------------------------------+
                    |
                    v
              Final SQL Query
```

---

## Phase 1: Decomposition

### Input

A typical metric request includes:
- **Metrics**: `["v3.total_revenue", "v3.avg_order_value", "v3.wow_revenue_change"]`
- **Dimensions**: `["v3.date.month", "v3.product.category"]`
- **Filters**: `["v3.date.year = 2024"]`

### Process

**1. Load nodes and build dependency graph:**

```
v3.total_revenue (base)      -> parent: v3.order_details (fact)
v3.order_count (base)        -> parent: v3.order_details (fact)
v3.avg_order_value (derived) -> parents: [v3.total_revenue, v3.order_count]
v3.wow_revenue_change (window) -> parents: [v3.total_revenue]
```

**2. Extract components from base metrics:**

For `v3.total_revenue = SELECT SUM(amount) FROM v3.order_details`:

```python
DecomposedMetricInfo(
    metric_node: v3.total_revenue,
    components: [
        MetricComponent(
            name="hash_abc123",
            expression="amount",
            aggregation="SUM",
            rule=AggregationRule(type=FULL, merge="SUM")
        )
    ],
    combiner_ast: SUM(amount),  # How to combine components
    aggregability: FULL
)
```

**3. Build parent_map:**

```python
parent_map = {
    "v3.total_revenue": ["v3.order_details"],      # fact parent
    "v3.order_count": ["v3.order_details"],        # fact parent
    "v3.avg_order_value": ["v3.total_revenue", "v3.order_count"],  # metric parents
    "v3.wow_revenue_change": ["v3.total_revenue"], # metric parent (window)
}
```

---

## Phase 2: Measures (Pre-Aggregation)

### Step 2.1: Group by Parent Fact -> MetricGroups

Metrics are grouped by their underlying fact table:

```python
MetricGroup(
    parent_node: v3.order_details,
    decomposed_metrics: [
        DecomposedMetricInfo(v3.total_revenue, ...),
        DecomposedMetricInfo(v3.order_count, ...),
    ]
)
```

If metrics come from different facts (e.g., `v3.order_details` and `v3.page_views`), they become separate MetricGroups.

### Step 2.2: Analyze Grain Groups by Aggregability

Within each MetricGroup, components are split by aggregability:

| Aggregability | Description | Example |
|--------------|-------------|---------|
| **FULL** | Can be summed at any grain | `SUM(amount)` -> merge with SUM |
| **LIMITED** | Requires specific grain | `COUNT(DISTINCT customer_id)` -> needs customer_id in grain |
| **NONE** | Cannot be pre-aggregated | `RANK() OVER (...)` -> pass through raw rows |

### Step 2.3: Build GrainGroupSQL (Pre-Aggregated CTEs)

For each GrainGroup, generate a CTE that aggregates components to the requested grain:

```sql
-- GrainGroupSQL for v3.order_details (FULL aggregability)
-- Aggregates at user's requested grain: (month, category)
WITH order_details_0 AS (
    SELECT
        d.month,
        p.category,
        SUM(od.amount) AS hash_abc123,      -- total_revenue component
        COUNT(*) AS hash_def456             -- order_count component
    FROM v3.order_details od
    LEFT JOIN v3.date d ON od.date_id = d.date_id
    LEFT JOIN v3.product p ON od.product_id = p.product_id
    WHERE d.year = 2024
    GROUP BY d.month, p.category
)
```

**GrainGroupSQL structure:**

```python
GrainGroupSQL(
    query: ast.Query,           # The CTE SQL
    columns: [                  # Column metadata
        ColumnMetadata(name="month", semantic_type="dimension"),
        ColumnMetadata(name="category", semantic_type="dimension"),
        ColumnMetadata(name="hash_abc123", semantic_type="metric_component"),
        ColumnMetadata(name="hash_def456", semantic_type="metric_component"),
    ],
    grain: ["month", "category"],
    aggregability: FULL,
    metrics: ["v3.total_revenue", "v3.order_count"],
    parent_name: "v3.order_details",
    component_aliases: {
        "hash_abc123": "hash_abc123",  # component_name -> SQL alias
        "hash_def456": "hash_def456",
    }
)
```

### Step 2.4: Merge Compatible Grain Groups

If multiple grain groups from the same parent have compatible aggregabilities, they can be merged to reduce JOINs:

```
Before merge:
  - GrainGroup(FULL, components=[SUM(amount)])
  - GrainGroup(LIMITED, components=[COUNT(DISTINCT customer_id)])

After merge:
  - GrainGroup(merged=True,
      grain_columns=[customer_id],  # Finest grain needed
      components=[SUM(amount), customer_id])  # Raw values at finest grain
```

The merged CTE outputs at finest grain; reaggregation happens in the metrics phase.

### Step 2.5: Build Window Metric Grain Groups

Window metrics (LAG/LEAD for WoW, MoM, etc.) need data at their ORDER BY grain, not necessarily the user's requested grain.

**Example:** User requests daily data, but `wow_revenue_change` needs weekly data for LAG comparison.

```python
# Extract window metric grains
window_metric_grains = {
    "v3.wow_revenue_change": {"v3.date.week"},  # ORDER BY grain
    "v3.mom_revenue_change": {"v3.date.month"}, # ORDER BY grain
}
```

**Grouping by (grain, parent_fact):**

Window metrics are grouped by both their ORDER BY grain AND their parent fact:

```python
# Key: (frozenset of grain cols, parent_name)
grain_parent_to_metrics = {
    ({"v3.date.week"}, "v3.order_details"): ["v3.wow_revenue_change"],
    ({"v3.date.week"}, "v3.page_views"): ["v3.wow_page_views_change"],
}
```

This ensures window metrics from different facts don't get incorrectly combined.

**Cross-fact detection:**

If a window metric references metrics from multiple facts:

```python
v3.wow_conversion_rate_change = LAG(conversion_rate, 1) OVER (ORDER BY week)
# where conversion_rate = orders / page_views (cross-fact)

is_cross_fact_window = True  # Uses base_metrics CTE as source
```

---

## Phase 3: Metrics (Combination & Final SQL)

### Step 3.1: Build base_metrics CTE

Combine all grain groups via FULL OUTER JOIN:

```sql
WITH
-- Grain group CTEs from measures phase
order_details_0 AS (...),
page_views_0 AS (...),

-- Combine grain groups
base_metrics AS (
    SELECT
        COALESCE(t0.month, t1.month) AS month,
        COALESCE(t0.category, t1.category) AS category,
        -- Apply combiner expressions to get final metrics
        t0.hash_abc123 AS total_revenue,                              -- SUM(amount)
        t0.hash_def456 AS order_count,                                -- COUNT(*)
        t0.hash_abc123 / NULLIF(t0.hash_def456, 0) AS avg_order_value, -- derived
        t1.hash_xyz789 AS page_views                                  -- from different fact
    FROM order_details_0 t0
    FULL OUTER JOIN page_views_0 t1
        ON t0.month = t1.month AND t0.category = t1.category
)
```

**Combiner expressions:**

For derived metrics, the combiner AST is applied:

```python
# v3.avg_order_value combiner_ast:
#   v3.total_revenue / NULLIF(v3.order_count, 0)
#
# Becomes:
#   t0.hash_abc123 / NULLIF(t0.hash_def456, 0)
```

### Step 3.2: Build Window CTEs (Reaggregation + Window Functions)

For each window grain group, we may need two CTEs:

**CTE 1: Aggregation CTE (reaggregate to coarser grain)**

Only needed when the user's grain is finer than the window's ORDER BY grain.

```sql
-- Reaggregate from base grain group to window grain (weekly)
order_details_week_agg AS (
    SELECT
        week,
        category,
        SUM(hash_abc123) AS total_revenue  -- Reaggregate component
    FROM order_details_0                   -- Source: base grain group CTE
    GROUP BY week, category
)
```

For **cross-fact window metrics**, the source is `base_metrics`:

```sql
-- Cross-fact: reaggregate from base_metrics (has combined metrics)
cross_fact_week_agg AS (
    SELECT
        week,
        category,
        SUM(total_revenue) AS total_revenue,
        SUM(page_views) AS page_views
    FROM base_metrics
    GROUP BY week, category
)
```

**CTE 2: Window CTE (apply LAG/LEAD)**

```sql
-- Apply window function
order_details_week AS (
    SELECT
        week,
        category,
        total_revenue,
        total_revenue - LAG(total_revenue, 1) OVER (
            PARTITION BY category
            ORDER BY week
        ) AS wow_revenue_change
    FROM order_details_week_agg
)
```

**Optimization: Skip intermediate CTEs when grain matches**

When the user's requested grain already matches the window's ORDER BY grain, the window function can be applied directly to `base_metrics`:

```sql
-- No intermediate CTEs needed - apply LAG directly
SELECT
    base_metrics.category,
    base_metrics.week,
    base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER (
        PARTITION BY base_metrics.category
        ORDER BY base_metrics.week
    ) AS wow_revenue_change
FROM base_metrics
```

### Step 3.3: Build Final SELECT

Join all CTEs together for the final result:

```sql
SELECT
    COALESCE(bm.month, w0.week) AS month,
    COALESCE(bm.category, w0.category) AS category,
    bm.total_revenue,
    bm.order_count,
    bm.avg_order_value,
    w0.wow_revenue_change
FROM base_metrics bm
FULL OUTER JOIN order_details_week w0
    ON bm.category = w0.category
```

---

## Complete Example: Multi-Metric Query

**Request:**

```python
metrics = [
    "v3.total_revenue",        # Base metric (FULL)
    "v3.unique_customers",     # Base metric (LIMITED - COUNT DISTINCT)
    "v3.avg_order_value",      # Derived metric
    "v3.wow_revenue_change",   # Window metric (WoW)
]
dimensions = ["v3.date.day", "v3.product.category"]
```

**Generated SQL:**

```sql
WITH
-- ==============================================================
-- GRAIN GROUP CTEs (from measures phase)
-- ==============================================================

-- Merged grain group: FULL + LIMITED at finest grain (includes customer_id)
order_details_0 AS (
    SELECT
        d.day,
        p.category,
        od.customer_id,                    -- Grain column for COUNT DISTINCT
        SUM(od.amount) AS hash_revenue,    -- total_revenue component
        COUNT(*) AS hash_orders            -- order_count component
    FROM v3.order_details od
    LEFT JOIN v3.date d ON od.date_id = d.date_id
    LEFT JOIN v3.product p ON od.product_id = p.product_id
    GROUP BY d.day, p.category, od.customer_id
),

-- ==============================================================
-- BASE_METRICS CTE (combine + apply combiners)
-- ==============================================================

base_metrics AS (
    SELECT
        day,
        category,
        SUM(hash_revenue) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,  -- LIMITED reaggregation
        SUM(hash_revenue) / NULLIF(SUM(hash_orders), 0) AS avg_order_value
    FROM order_details_0
    GROUP BY day, category
),

-- ==============================================================
-- WINDOW METRIC CTEs (reaggregate to coarser grain + apply window)
-- ==============================================================

-- Step 1: Reaggregate to weekly grain (from base grain group, not base_metrics)
order_details_week_agg AS (
    SELECT
        DATE_TRUNC('week', day) AS week,   -- Coarser grain
        category,
        SUM(hash_revenue) AS total_revenue -- Reaggregate from components
    FROM order_details_0
    GROUP BY DATE_TRUNC('week', day), category
),

-- Step 2: Apply LAG window function
order_details_week AS (
    SELECT
        week,
        category,
        total_revenue,
        total_revenue - LAG(total_revenue, 1) OVER (
            PARTITION BY category
            ORDER BY week
        ) AS wow_revenue_change
    FROM order_details_week_agg
)

-- ==============================================================
-- FINAL SELECT
-- ==============================================================

SELECT
    bm.day,
    bm.category,
    bm.total_revenue,
    bm.unique_customers,
    bm.avg_order_value,
    w.wow_revenue_change
FROM base_metrics bm
LEFT JOIN order_details_week w
    ON bm.category = w.category
    AND DATE_TRUNC('week', bm.day) = w.week
```

---

## Key Design Decisions

### 1. Why Grain Groups?

Grain groups enable:
- **Pre-aggregation matching**: If a materialized pre-agg exists at the right grain, use it instead of scanning source tables
- **Efficient JOINs**: Metrics from the same fact are computed together in one CTE
- **Proper aggregability handling**: LIMITED/NONE metrics get special treatment

### 2. Why Reaggregate from Grain Groups (not base_metrics)?

For **single-fact window metrics**, we reaggregate from the grain group CTE:

```
Grain Group CTE (has components)
    -> Reaggregate components
    -> Apply window function
```

This is more efficient than reaggregating from base_metrics because:
- Components (SUM, COUNT) can be correctly summed
- Derived metrics (ratios) CANNOT be summed

Example: `avg_order_value = revenue / orders` cannot be summed! You can't add averages together.

### 3. Why Group Window Metrics by (grain, parent_fact)?

Without parent_fact grouping, window metrics with the same ORDER BY grain but different parent facts would be incorrectly combined:

```
WoW metrics from thumb_rating AND download_session
    -> Same weekly grain
    -> Incorrectly combined!
    -> SUM(thumbs_up_rate) is WRONG (can't sum a rate)
```

With parent_fact grouping:

```
("week", "thumb_rating"): [wow_thumbs_up_rate]     -> Separate CTE
("week", "download_session"): [wow_download_time]  -> Separate CTE
```

### 4. When to Use base_metrics as Source?

**Cross-fact window metrics** must use `base_metrics`:

```python
wow_conversion_rate = LAG(conversion_rate, 1) OVER (...)
# where conversion_rate = orders / page_views

# conversion_rate spans two facts, so:
# - Cannot reaggregate from single grain group
# - Must read from base_metrics which has the combined metric
```

The `is_cross_fact_window` flag on GrainGroupSQL controls this routing.

---

## Metric Types Summary

| Metric Type | Example | SQL Generation |
|-------------|---------|----------------|
| **Base** | `SUM(amount)` | Component in grain group CTE |
| **Derived** | `revenue / orders` | Combiner applied in base_metrics |
| **Window** | `LAG(revenue, 1)` | Separate CTE with window function |
| **Window (cross-fact)** | `LAG(orders/page_views, 1)` | Uses base_metrics as source |

---

## Code References

The SQL generation logic lives in `datajunction_server/construction/build_v3/`:

| File | Responsibility |
|------|----------------|
| `builder.py` | Entry point, orchestrates the phases |
| `decomposition.py` | Extracts components from metrics |
| `measures.py` | Builds grain group CTEs |
| `metrics.py` | Combines grain groups, builds final SQL |
| `types.py` | Data structures (GrainGroupSQL, DecomposedMetricInfo, etc.) |
| `cte.py` | CTE building utilities |
