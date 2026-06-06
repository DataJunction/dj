---
name: datajunction-semantic-model
description: |
  Activate this skill for DataJunction (DJ) semantic modeling decisions —
  choosing the right node shape (fact, dimension, transform, metric, cube),
  turning a draft SQL query into well-designed nodes, and the cross-cutting
  conventions (ownership, naming, namespace organization). Format-agnostic
  modeling guidance; for YAML schemas and the repo-backed authoring flow,
  invoke `datajunction-repo`; for direct API examples, invoke
  `datajunction-api`. For DJ concepts (node types, dim links), invoke
  `datajunction`. For querying existing metrics, invoke
  `datajunction-query`.
  Keywords:
  - semantic modeling
  - decompose query, model query, query to nodes
  - ratio metric, derived metric, base metric
  - composable metrics
  - metric query constraints
  - node ownership, metric ownership
  - metric naming, namespace organization
  - grain, fact vs dimension
  - dimension link, not JOIN
user-invocable: false
---

# DataJunction Semantic Modeling

Use this skill when designing how something gets expressed as DJ nodes — independent of whether you'll write the result as YAML files in a repo or POST it to the DJ API.

For DJ vocabulary (what node types exist, how dimension links work mechanically), see the `datajunction` skill. This skill assumes that vocabulary and focuses on the modeling *decisions*.

---

## General principles

### Reuse over reinvent

Before authoring a new node, check whether an authoritative one already exists. Use the `datajunction-query` skill's MCP tools (`search_nodes`, `get_node_details`) to find candidates. Every additional transform / dim / metric fragments the catalog; every reuse strengthens it.

When you can't find a fit:
- **Build on the closest existing parent** rather than creating a sibling source/transform that duplicates upstream logic.
- **Refuse to build on non-git-backed namespaces.** Nodes without a source repo can change or disappear without review — depending on them silently couples your work to someone's UI session. Stick to git-backed namespaces (see `datajunction-query` for how to check).

### Every node has owners

Not just metrics — every DJ node should declare owners. The reasons apply uniformly across all node types:
- No one to contact when the node breaks or produces unexpected values
- Undefined responsibility for keeping definitions accurate
- Difficult to coordinate changes or deprecations
- Team knowledge scattered across individuals

**Best practices:**
- ✅ **Prefer team emails over individuals** — teams outlast individual contributors
- ✅ Use Google groups or distribution lists for teams
- ✅ Include both data team AND business stakeholder teams where applicable
- ❌ Never leave `owners` field empty or omit it

```
✅ GOOD — team ownership:
  data-platform-team@company.com
  finance-analytics@company.com

⚠️ ACCEPTABLE — individual ownership (less sustainable):
  alice@company.com

❌ BAD — no owners (governance nightmare!)
```

This applies to **sources, transforms, dimensions, metrics, and cubes** alike.

### Naming conventions

Use fully qualified names with namespace:

```
namespace.node_name
```

**Examples:**
- ✅ `finance.total_revenue`
- ✅ `common.dimensions.users`
- ✅ `clean.user_events`
- ❌ `revenue` (missing namespace)

Names should be **readable and business-meaningful** — what a stakeholder would call this thing, not the column transformations behind it. `total_signup_revenue` over `sum_signup_plan_usd_price`. `avg_session_duration_secs` over `avg_session_dur`.

### Namespace organization

Namespaces are organized by business area:

**Common conventions:**
- `common.dimensions.*` — shared dimensions (users, dates, regions)
- `finance.*` — financial metrics & facts
- `growth.*` — user engagement & activation
- `product.*` — product usage & features
- `source.*` — raw source tables

### Model named, reusable entities — not anonymous SQL

A general spirit that runs through every layer:
- Don't bury an aggregate inside a derived metric's query when it should be its own named base metric (see the **Metrics** section below for the reusability rule).
- Don't bury a join condition inside a query when it should be a dimension link (see "Joins → dimension links" below).

The semantic layer has two goals working together:
- **Express the business in business concepts.** Stakeholders should see *signups*, *revenue*, *active customers* — not a chain of tables and SQL transformations. Every node should have a name and meaning a non-engineer would recognize.
- **Make every meaningful thing findable, queryable, and reusable.** Anonymous SQL — inline aggregates, hardcoded joins — works against both goals: it hides business concepts behind technical detail and forecloses reuse.

---

## The dimensional model

DJ uses normalized star schema modeling. The decisions you make here shape everything downstream.

### Facts vs dimensions vs transforms

| Node | Used for | Examples |
|---|---|---|
| **Source** | Physical table in the warehouse | `prodhive.dse.transactions_table` |
| **Dimension** | An entity with attributes you'll slice by | `users`, `products`, `dates`, `regions`, `geo_country` |
| **Transform** | Cleaned/derived fact data — the aggregable rows | `clean_transactions`, `daily_user_activity`, `signup_funnel_base` |
| **Metric** | One aggregation expression over a transform | `total_revenue`, `num_signups`, `avg_session_duration` |
| **Cube** | A curated set of metrics + dimensions for downstream consumers | `revenue_dashboard`, `signup_funnel_report` |

**When to author a transform vs use a source directly:**

- Use the source directly when its row shape *is* the semantic entity you want to aggregate.
- Author a transform when the source needs cleaning (`status IN ('complete', 'completed')` → `'completed'`), joining, or filtering before it represents the entity meaningfully.
- The transform is where you attach dimension links — that determines what slices are available to every metric built on it.

### Grain is the most important decision in a fact

What does *one row* in your fact transform represent?
- "One row per signup" → grain is `subscrn_id`
- "One row per signup per snapshot day" → grain is `(subscrn_id, snapshot_date)`
- "One row per account per month" → grain is `(account_id, month)`

Grains don't mix in one fact. If you find yourself with two different grains in one transform, you have two facts trying to live in one node — split them.

### Joins → dimension links, not baked-in JOINs

The DJ idiom: every JOIN that connects a fact to a dimension belongs in a `dimension_links:` declaration on the fact's transform — *not* inside a metric's query.

Why: dimension links make the join *optional*. Consumers slice by that dim only when they ask for it. A hardcoded JOIN in a metric forces every query that touches the fact to pay the join cost even when nobody's slicing by that dim. It also fragments behavior — metrics on the same fact would each carry their own copy of the same JOIN, with the inevitable drift.

Same applies to dim-to-dim joins (e.g., `users → countries → regions`) — express them as dimension links on the dimension node, creating a dimensional graph DJ can traverse automatically.

See the `datajunction` skill's "Dimension Links" section for the mechanics; this is about *when* to use them — which is essentially always.

---

## Metrics

A metric is **one aggregation expression** over a single source, transform, or dimension node.

### Metric query constraints

**Metrics cannot contain WHERE clauses**
- Use CASE WHEN for conditional aggregation instead

```sql
-- ❌ NOT ALLOWED — WHERE clause in metric
SELECT SUM(amount_usd)
FROM finance.transactions
WHERE status = 'completed'

-- ✅ CORRECT — use CASE WHEN
SELECT SUM(
  CASE WHEN status = 'completed' THEN amount_usd ELSE 0 END
) FROM finance.transactions
```

A `WHERE` permanently constrains scope; `CASE WHEN` lets the metric coexist with a broader-population sibling on the same fact. Reflect the scope in the metric name (`active_signups`, not `signups`).

**Metrics select a single expression from a single node**
- Cannot join multiple nodes in a metric query
- Define joins via dimension links on the source/transform node instead

### Metrics can reference other metrics (composition)

Build derived metrics by referencing other metrics in the query. Excellent for ratios, rates, and complex calculations.

```sql
-- Create base metrics first
SELECT COUNT(*) FROM finance.transactions              -- metric: finance.transaction_count
SELECT SUM(amount_usd) FROM finance.transactions       -- metric: finance.total_revenue

-- Then compose them into a derived metric
SELECT finance.total_revenue / finance.transaction_count  -- metric: finance.avg_transaction_value
-- DJ handles divide-by-zero automatically; NULLIF() is optional safety
```

### The reusability rule: every aggregate is its own named metric

This is the highest-leverage discipline in DJ metric modeling. Apply it even when "the user only cares about the final ratio."

❌ Less reusable — anonymous SQL blob:
```sql
SELECT SUM(signup_price)
       / NULLIF(SUM(CASE WHEN subscrn_id IS NOT NULL THEN 1 ELSE 0 END), 0)
FROM acquisition.signup_funnel_base
```

✅ Reusable — three named, discoverable, composable metrics:
```sql
-- metric: total_signup_revenue
SELECT SUM(signup_price) FROM acquisition.signup_funnel_base

-- metric: num_signups
SELECT SUM(CASE WHEN subscrn_id IS NOT NULL THEN 1 ELSE 0 END)
FROM acquisition.signup_funnel_base

-- metric: avg_signup_price (derived)
SELECT total_signup_revenue / NULLIF(num_signups, 0)
```

Why: the bottom shape gives three named metrics anyone can find, query, and reuse. The top shape gives one — the base aggregates are anonymous SQL blobs nobody else can reference. Different teams asking "what's our total signup revenue?" can't find it; they'd have to know to read inside `avg_signup_price`'s query.

### Metric patterns

Common shapes a metric can take. Each shows the `query:` expression — the actual file/API format lives in `datajunction-repo` or `datajunction-api`.

**Base metrics (simple aggregations):**
```sql
-- COUNT
SELECT COUNT(transaction_id) FROM finance.transactions

-- COUNT DISTINCT
SELECT COUNT(DISTINCT customer_id) FROM finance.transactions

-- APPROX_COUNT_DISTINCT (HyperLogLog — for large datasets)
SELECT APPROX_COUNT_DISTINCT(profile_id) FROM finance.transactions

-- SUM
SELECT SUM(amount_usd) FROM finance.transactions

-- AVG
SELECT AVG(amount_usd) FROM finance.transactions

-- Conditional aggregation
SELECT SUM(
  CASE
    WHEN status = 'completed' AND refund_flag = false
    THEN amount_usd
    ELSE 0
  END
) FROM finance.transactions
```

**Derived metrics (composed from base metrics):**
```sql
-- ratio
SELECT finance.total_revenue / NULLIF(finance.transaction_count, 0)

-- rate as percentage
SELECT finance.clicks * 100.0 / NULLIF(finance.impressions, 0)

-- revenue per thousand impressions (RPM)
SELECT finance.total_revenue / NULLIF(finance.impressions, 0) * 1000
```

**Statistical metrics:**
```sql
SELECT VAR_POP(amount_usd) FROM finance.transactions
SELECT STDDEV_POP(amount_usd) FROM finance.transactions
SELECT PERCENTILE_APPROX(amount_usd, 0.5)  FROM finance.transactions  -- median
SELECT PERCENTILE_APPROX(amount_usd, 0.95) FROM finance.transactions  -- p95
```

**Rolling window metrics** (set `required_dimensions` to include the ORDER BY dim):
```sql
-- trailing 7-day sum
SELECT SUM(finance.daily_revenue) OVER (
  ORDER BY common.dimensions.date.dateint
  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

**Period-over-period metrics:**
```sql
-- week-over-week % change
SELECT
  (finance.weekly_revenue - LAG(finance.weekly_revenue, 1) OVER (
    ORDER BY common.dimensions.date.week_code
  )) * 100.0 /
  LAG(finance.weekly_revenue, 1) OVER (
    ORDER BY common.dimensions.date.week_code
  )
```

Same shape for MoM (`month_code`), QoQ (`quarter_code`), YoY (`year`).

### Metric metadata quick reference

| Field | Required | Valid Values | Notes |
|-------|----------|--------------|-------|
| `name` | ✅ Yes | `namespace.metric_name` | Fully qualified name |
| `query` | ✅ Yes | SQL SELECT expression | Single aggregation from single node |
| `description` | ❌ Optional | String | Recommended for clarity |
| `direction` | ❌ Optional | `higher_is_better` / `lower_is_better` / `neutral` | Indicates performance direction |
| `unit` | ❌ Optional | `dollar` / `unitless` / **⚠️ NOT `count`** | Server rejects `count` — use `unitless` |
| `mode` | ❌ Optional | `draft` / `published` | Default: `published` |
| `required_dimensions` | ❌ Optional | List of dimension names | For time-based / windowed metrics |
| `owners` | ❌ Optional but strongly recommended | List of email addresses | Prefer team emails |

### Key patterns summary

- **DJ handles divide-by-zero automatically** — NULLIF() is optional safety
- **Use CASE WHEN** instead of WHERE clauses for filtering
- **Window functions** enable rolling windows and period-over-period comparisons
- **required_dimensions** should include the dimension used in window ORDER BY clauses
- **Derived metrics** can reference other metrics for ratios and calculations
- **Always specify owners** — use team emails for sustainability

---

## Query → DJ decomposition workflow

When a user arrives with an existing SQL query and wants to express it as DJ nodes, **don't generate node definitions on the first pass**. Propose a structured decomposition, get critique, iterate on the shape, then have the user create the nodes (in YAML via `datajunction-repo`, or via API via `datajunction-api`).

### The workflow

**1. Parse the query mechanically.** From the user's SQL, extract:
- **Aggregates** (`SUM`, `COUNT`, `COUNT DISTINCT`, `MIN`, `MAX`, `AVG`,
  `APPROX_COUNT_DISTINCT`, percentile/window aggregates) → each is a
  **candidate base metric**.
- **FROM / JOIN tables** → candidate parent nodes (transforms or dimensions).
- **GROUP BY columns, JOIN-bound columns, top-level WHERE columns** →
  candidate **dimension link** targets.
- **Top-level SELECT expressions that combine the aggregates** (ratios,
  sums-of-sums, computed expressions) → candidate **derived metrics**.

**2. Resolve parents against existing nodes first.** For each candidate parent table or dimension, use the `datajunction-query` skill's MCP tools (`search_nodes`, `get_node_details`) to check whether DJ already has an authoritative node for it. Prefer building on existing nodes — every additional transform fragments the catalog.

**3. Treat every JOIN as a candidate dimension link, not a baked-in join.** See "Joins → dimension links" above. The DJ idiom is to express joins as `dimension_links:` on the fact's transform, *not* hardcoded in the metric query.

**4. Apply the reusability rule.** Even if the user's query has aggregates *only* inside a ratio expression, decompose them: one named base metric per aggregate, then a derived metric for the ratio.

**5. Handle WHERE clauses correctly.** A `WHERE` in the user's query is one of two things:
- A **dimensional filter** the consumer should apply at query time → keep it out of the metric definition entirely.
- **Part of the metric's identity** → bake it into the aggregate as `CASE WHEN`, not as a `WHERE` on the metric's query.

**6. Name things meaningfully.** Propose readable, business-meaningful names that match what a stakeholder would call the metric or entity.

**7. Check for duplicates before producing nodes.** For each proposed metric / transform / dimension name, check whether something with the same name (or doing the same thing under a different name) already exists in the catalog. Reuse rather than recreate; rename when the names collide but the semantics differ.

**8. Propose, don't produce.** Present the decomposition as a structured list (parents, base metrics, derived metrics, dim links) and ask the user to critique the shape. Only after they confirm, hand off to `datajunction-repo` (for YAML) or `datajunction-api` (for curl) to produce the actual node definitions.

### Worked example: ratio decomposition

User's query:

```sql
SELECT
  cell_id,
  SUM(signup_plan_usd_price)
  / SUM(CASE WHEN subscrn_id IS NOT NULL THEN 1 ELSE 0 END) AS asp
FROM acquisition.signup_funnel_base
GROUP BY cell_id
```

Decomposition:

```
Parent transform:
  Reuse: acquisition.signup_funnel_base  (existing)

Base metrics (one per aggregate):
  1. total_signup_revenue = SUM(signup_plan_usd_price)
  2. num_signups          = SUM(CASE WHEN subscrn_id IS NOT NULL THEN 1 ELSE 0 END)

Derived metric:
  1. avg_signup_price = total_signup_revenue / NULLIF(num_signups, 0)

Dimension links:
  - cell_id → common.dimensions.xp.ab_test_cell  (existing shared dim)
```

Why this shape is worth the extra metric nodes: downstream tools re-aggregate the materialized output. Three independent metrics roll up correctly across any dimensional slice — sum the numerator, sum the denominator, then divide. A single pre-divided ratio doesn't compose: when consumers re-aggregate, the numerator and denominator can drift apart across slices in subtle ways (especially when NULL rows are dropped from one but not the other).
