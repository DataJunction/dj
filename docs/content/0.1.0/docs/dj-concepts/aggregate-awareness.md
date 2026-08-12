---
weight: 86
title: "Query Routing & Aggregate Awareness"
mermaid: true
---

Query routing is how DataJunction decides where a metric query reads from: a pre-aggregated
(materialized) dataset, or a live computation from source tables. When a suitable materialization
exists, DJ routes the query to it automatically — you don't ask for a specific cube or
pre-aggregation, you just request metrics and dimensions and DJ picks the best available source.
This automatic selection of a pre-aggregated source is called **aggregate awareness**.

## How routing chooses a source

When you request metrics and dimensions (e.g. via the SQL or data endpoints), DJ decides which
tables to read from based on the aggregates available.

An aggregate becomes available in one of two ways: **DJ materializes it for you** — a cube or
pre-aggregation it builds and keeps refreshed — or **you register an externally-built table** that an
outside pipeline already produces. Routing treats both the same. Registering an external table is
described [below](#registering-externally-built-pre-aggregations); for DJ-managed materialization see
[Materialization](../materialization/).

Say `total_revenue` has an aggregate at `order_date × region` grain. Different requests route
differently:

{{< mermaid class="bg-light text-center" >}}
graph LR
  Q1["total_revenue<br/>by region"] -->|rolls up over order_date| A["aggregate<br/>order_date × region"]
  Q2["total_revenue<br/>by order_date, region"] -->|exact grain| A
  Q3["total_revenue<br/>by product"] -->|product not in<br/>the aggregate| S["source tables<br/>computed live"]
  style A fill:#79d60080,stroke:#ccc,stroke-width:1px
  style S fill:#21b8ff40,stroke:#ccc,stroke-width:1px
{{< /mermaid >}}

When more than one aggregate can serve a request, DJ picks the coarsest that still covers it (the
fewest dimensions to roll up). The match is decided fresh on each request against whatever aggregates
currently exist and have data, so a new one becomes usable the moment it's ready.

> **Under the hood:** each pre-aggregation does persist an indexed `grain_group_hash` and a unique
> `preagg_hash` (derived from the parent node, grain, and measure expression hashes). Today these are
> an **identity/deduplication** mechanism — used when a pre-aggregation is created, to recognize that
> an equivalent one already exists — rather than a structure the query path looks up against. The
> metrics-query path loads the materialized pre-aggregations for the parent node and matches grain and
> measures in memory.

## Two layers of matching

DJ matches at two granularities. A query can be served by a full **cube** or, failing that, by
**pre-aggregations** of the underlying measures. These are two distinct matchers — they match on
different keys and sit at different altitudes — but they're related by **layering**: a cube is itself
built from pre-aggregations (its materialization is a combiner over the per-parent pre-aggregation
tables), and pre-aggregations are the shared building blocks. So a cube isn't a special kind of
pre-aggregation; it's a higher-level rollup assembled from them.

### Cube matching

A cube is a materialized table holding a fixed set of metrics across a fixed set of dimensions. A cube
is eligible to serve a query when:

1. It contains **every requested metric** (the cube's metrics are a superset of the request), and
2. It contains **every requested dimension** (the cube's dimensions are a superset of the request), and
3. It has an availability state — i.e. it has actually been materialized and has data.

When more than one cube qualifies, DJ picks the one with the **fewest dimensions**. A smaller cube
means less data to roll up, so it's the cheapest source that still covers the request.

Cube matching itself is **store-agnostic** — it matches on metrics, dimensions, and availability, not
on where the table lives. A deployment that materializes cubes into an OLAP store like Druid gets
served from there; a cube materialized to a warehouse table (e.g. Iceberg) is matched and read the
same way. Druid is a common deployment choice, not a requirement of cube matching.

### Pre-aggregation matching

Pre-aggregations are materialized tables of a node's decomposed **measures** (see
[Metric Decomposition](../metric-decomposition/)) at some grain, rather than full cubes. They give DJ
a finer-grained fallback when no whole cube fits. A pre-aggregation is eligible when:

1. It belongs to the same parent node, and
2. The requested **grain is a subset of the pre-aggregation's grain** — so DJ can roll the
   pre-aggregated rows up to the requested grain, and
3. It contains **all the measures** the requested metrics decompose into, and
4. It has data available.

A requested dimension that *isn't* in the grain can still be covered when the grain holds that
dimension's **whole primary key**, at the same role, reachable over a single `LEFT` or `INNER` link: DJ
joins the dimension back on the retained key and groups by the attribute. The whole key is required
because retaining only part of a composite key would match several dimension rows per aggregated row and
multiply the measures — so a daily-snapshot dimension keyed `(account_id, utc_date)` needs both columns
registered, even when the aggregate's date is already present as another dimension's column.

Measures are matched by **expression and aggregation**, not by name — two metrics that decompose to the
same expression aggregated the same way share a pre-aggregation, while `SUM(x)` and `MAX(x)` are distinct
measures that can't stand in for each other. As with cubes, when several pre-aggregations qualify, DJ
chooses the **smallest grain** that covers the request.

Dimension references are compared **canonically**, so when exactly one role reaches a dimension, a bare
name and its role-qualified spelling match each other — a pre-aggregation registered with one spelling
still serves queries written with the other, including its `dimension_columns` column mapping. If the
dimension is also reachable by a role-free link, the bare name means that link and the two spellings
stay distinct, since that's how the reference resolves everywhere else. A bare name is only rejected
when the dimension is reachable by more than one role, since then it identifies none of them: a fact
that links a location dimension as both `[from]` and `[to]` has to be registered under one of the
role-qualified references, which registration lists for you in the error. Output column aliases are
unaffected — they still follow the spelling the caller used (`country` versus `country_from`), since
canonicalization applies to matching only.

Filters are applied wherever they're still correct. A filter on a dimension the query also groups by is
applied over the pre-aggregated rows; a filter on a dimension that's in the pre-aggregation's grain but
*not* in the requested output is pushed into the scan and applied **before** the roll-up, so the
predicate still selects rows rather than being lost when that dimension is aggregated away. A filter on a
dimension the pre-aggregation doesn't carry at all makes it ineligible — that predicate can only be
evaluated against rows the pre-aggregation has already collapsed.

## Materialized vs. live routing

Putting it together, when a metric query arrives DJ resolves where to read from:

1. If materialization is enabled for the request and a **cube** with availability matches, the query
   reads directly from the cube.
2. Otherwise DJ builds the query **live**, substituting any matching **pre-aggregation** tables in
   place of source scans where it can, and falling back to the source tables for the rest.

A few things that influence this:

- **`use_materialized`** (default on) — turning it off forces a live build from source, bypassing
  cube and pre-aggregation matching. Materialization-refresh jobs use this so a cube doesn't try to
  build itself from itself.
- **Dialect** — cube serving resolves to the cube's own materialization engine (in a Druid-backed
  deployment, that's Druid). Requesting a query in a different, incompatible dialect skips cube
  matching and resolves against the metric's own catalog engine instead.
- **Explicit cube** — callers that already know which cube they want can name it directly, which
  skips the matching scan.

## What this means when you model

Because routing is decided by the superset/subset rules above, you get the most out of aggregate
awareness by materializing at the **right grain**:

- A query is only served by a cube if that cube covers **all** of its metrics **and** dimensions. A
  dimension that isn't in the cube forces a live build (or a different source), even if everything
  else matches.
- Smaller-grain materializations are preferred and can serve any coarser request by rolling up, so a
  few well-chosen grains can cover many queries. Materializing at an unnecessarily fine grain costs
  storage and build time; too coarse and common queries miss it.
- Measures shared across metrics (same expression, aggregated the same way) are matched by identity,
  so one pre-aggregation can back several metrics.

For how metrics are broken into the additive measures that pre-aggregations store, see
[Metric Decomposition](../metric-decomposition/); for how materializations are configured, see
[Materialization](../materialization/).

## Registering externally-built pre-aggregations

Everything above assumes DJ built the pre-aggregation itself, but the matcher doesn't care who built
the table — only that its shape and grain are known. So you can register a table an external pipeline
already produces: you tell DJ where it lives and how its columns map to measures, and routing then
treats it exactly like a pre-aggregation DJ built.

### The core modeling rule: only measures can be mapped

The one thing to get right when registering an external table is that **you can only map physical
columns to measures, not to metrics in general.** Recall from
[Metric Decomposition](../metric-decomposition/) that a metric is a *measure* when its query is a single
aggregation that decomposes into exactly one storable component — `SUM(x)`, `COUNT(x)`, `MIN(x)`,
`MAX(x)`, or `COUNT(DISTINCT x)`. Anything that decomposes into more than one component, like `AVG(x)`
(which needs a sum and a count) or a ratio between two other metrics, isn't a measure — there's no
single column in your external table that could hold it.

In practice this means: model your ratios and averages as **derived metrics that reference base measure
metrics**, rather than trying to register them directly. A derived metric doesn't need its own column
mapping, because it's covered automatically once its component measures are covered. This is the same
"every aggregate is its own named metric" principle that underlies decomposition generally — you're
just applying it at registration time instead of at materialization time.

### Plan for this when you author metrics, not when you register tables

This constraint reaches back into how you model, because it cannot be worked around
later. `measure_columns` maps a **metric to a single column**, so a metric that
decomposes into more than one component has nothing to map and registration refuses
it. And the components it *does* decompose into are named automatically, with a hash
suffix derived from the expression — `line_total_sum_e1f61696`,
`customer_id_hll_23002251` — names that are deliberately not addressable from YAML.

So a metric authored as

```sql
SELECT SUM(revenue) / COUNT(DISTINCT view_id) FROM fct_views
```

can never be bound to an aggregate table, no matter what columns that table holds.
There is no spelling of `measure_columns` that reaches its two components. The only
fix is to refactor the metric, which means changing a node other teams may already
be querying.

The habit that avoids this: **give every aggregation primitive its own metric node**,
then compose. One node per `SUM(...)`, `COUNT(...)`, `COUNT(DISTINCT ...)`, and
ratios or averages expressed as derived metrics referencing those:

```yaml
# revenue.yaml          query: SELECT SUM(revenue) FROM fct_views
# view_count.yaml       query: SELECT COUNT(DISTINCT view_id) FROM fct_views
# revenue_per_view.yaml query: SELECT revenue / view_count      <- derived
```

Each primitive is independently mappable, and the derived metric is covered for free
once its components are. This costs nothing if you do it from the start and is
expensive to retrofit, so it is worth doing even for metrics you have no aggregate
table for yet.

### A measure is its expression *and* its aggregation

A measure is identified by the pair (expression, aggregation), not by the expression alone. `SUM(price)`
and `MAX(price)` are two different measures that happen to share an inner expression, and a stored
partial is only reusable by a metric that accumulates the same way — you can re-aggregate summed
partials with `SUM` and maxima with `MAX`, but you can't recover a maximum from a column of sums.

Two consequences when you register a table:

- **One table can back several aggregations of the same column**, each with its own mapping. If your
  table stores both a total and a peak, map them separately and both bindings are kept:

  ```yaml
  measure_columns:
    ${prefix}total_price: price_sum
    ${prefix}peak_price: price_max
  ```

- **A metric will not bind a column registered for a different aggregation.** If your table only stores
  `price_sum`, a `MAX(price)` metric won't read it — that query builds from the source instead. This is
  what stops a maximum from being silently computed over pre-summed values.

So map each metric to the column that was built with *that metric's* aggregation. Mapping `MAX(price)`
to a column holding sums is a modeling error DJ can't detect: the aggregation functions are compared,
but the column's data isn't.

One shape catches people out: a column backing `COUNT(DISTINCT x)` must hold the **raw values being
counted** — one row per distinct value at that grain — not a pre-computed count. DJ re-applies
`COUNT(DISTINCT ...)` to whatever the column holds, so mapping an already-counted `distinct_users`
integer produces a count of counts.

### Registering a table

There are two ways to tell DJ about an externally-built table: a one-off REST call, or a declarative
YAML file that lives alongside your other node definitions in a deployment. The YAML path is recommended
for anything you intend to keep around, since it's versioned with the rest of your semantic model and
gets reconciled on every deploy.

#### REST: `POST /preaggs/register`

Send DJ where the table lives, plus the metrics and dimensions it covers — each one named together with
the physical column that holds it:

```json
{
  "metrics": {
    "default.view_secs": "view_secs_sum",
    "default.session_count": "session_cnt"
  },
  "dimensions": {
    "default.page_d.page_id": "page",
    "default.geo_country_d.country_iso_code": "country_iso_code"
  },
  "table": {
    "catalog": "warehouse",
    "schema": "analytics",
    "table": "views_by_page_daily"
  }
}
```

Both maps require a value for every key, so `page_d.page_id` says it is stored as `page`, and
`geo_country_d.country_iso_code`, stored under the very name DJ uses, says that too rather than leaving
the value off. This is the same declaration the YAML spec below makes, in the same shape — the two paths
differ in where the file lives and when it is reconciled, not in what you write.

What goes under `metrics` are the *measures* the table physically stores. A ratio metric such as
`view_rate` has no column of its own, so it can't be named here — and doesn't need to be, for the reason
[the YAML section](#yaml-a-kind-preagg-file) spells out: declaring `view_secs` and `session_count` is
what covers everything built on them.

On registration DJ decomposes the metrics you named, then validates every binding: it confirms each key
under `metrics` really is a measure, and checks (via query-service introspection) that every column you
named actually exists in the table **and is type-compatible with the metric or dimension it backs** — a
numeric `SUM` can't bind to a string column, for instance (the check is category-level, so `int` vs
`bigint` vs `decimal` are all fine). If all of that checks out, DJ records the pre-aggregation. If you
also pass a `valid_through_ts`, DJ marks it available immediately so routing can start using it right
away.

#### YAML: a `kind: preagg` file

For anything durable, define the pre-aggregation as a deployment artifact instead. Every file in a
deployment declares a `kind`: files with no `kind` are nodes (the default), and a pre-aggregation
sets `kind: preagg`. The DJ client routes each `kind: preagg` file into the deployment's
pre-aggregations alongside your nodes, cubes, and other definitions.

Given these two measure metrics and one derived metric:

```yaml
# view_secs.yaml
node_type: metric
description: Total time spent viewing
query: SELECT SUM(view_secs) FROM ${prefix}fct_views
```

```yaml
# session_count.yaml
node_type: metric
description: Number of viewing sessions
query: SELECT COUNT(session_id) FROM ${prefix}fct_views
```

```yaml
# view_rate.yaml
node_type: metric
description: Average view seconds per session
query: SELECT ${prefix}view_secs / ${prefix}session_count
```

the pre-aggregation spec that binds `view_secs` and `session_count` to an externally-built table looks
like this:

```yaml
# views_by_page.yaml
kind: preagg
name: views_by_page
catalog: warehouse
schema: analytics
table: views_by_page_daily
metrics:
  ${prefix}view_secs: view_secs_sum
  ${prefix}session_count: session_cnt
dimensions:
  ${prefix}page_d.page_id: page
  ${prefix}geo_country_d.country_iso_code: country_iso_code
```

Every metric and every dimension is written together with the physical column that holds it, so a
reference and its binding can never drift apart, and reading the file tells you what the table looks
like. Both maps require a value for every key. `page_d.page_id` is stored as `page`, so it says so;
`geo_country_d.country_iso_code` happens to be stored under the same name DJ uses, and it says that
too rather than leaving the value off. That is deliberate — a trailing colon is easy to write by
accident, and an optional value would make the map not really a mapping. Each binding is validated
the same way — the column must exist and be type-compatible — and only changes how the table is read,
not how it's matched. A bound dimension can even be a joined attribute the table has denormalized (say
it stores `country` directly rather than an account key you'd otherwise join through), which DJ then
reads straight from the table with no join.

Notice that `view_rate` doesn't appear in the file at all. What you declare under `metrics` are the
*measures* the table physically stores, and a derived metric has no column of its own to name, so it
can't be listed. It doesn't need to be: both registration and query-time routing work on decomposed
measures rather than metric names, so any metric that resolves to `view_secs` and `session_count` — the
ratio `view_rate` among them — is served by this table automatically. Declare the measures and the
metrics built on them follow.

The earlier form of this file, where `metrics` and `dimensions` were plain lists and the columns lived
in separate `measure_columns` and `dimension_columns` blocks, is no longer accepted; a file still using
it is rejected with a message describing what to write instead. If you have one, move each column up
next to the reference it belongs to, and write out the columns for any dimensions that were previously
left unmapped and relied on DJ's column name.

On deploy, DJ registers any pre-aggregation specs it finds. Because deployments are the source of truth,
it also removes a previously-registered pre-aggregation once you drop its spec from a deploy that still
declares others — the same way removing a node file deletes that node. As a safeguard against an
accidental or partial push wiping externally-managed tables, a deploy that declares *no* pre-aggregations
at all never mass-deregisters the existing ones; removing your last one is an explicit action, done by
passing `allow_empty` on the deploy.

### Freshness is reported separately from the binding

You'll notice the YAML spec above has no `valid_through_ts`. That's deliberate: the YAML describes the
durable *binding* — which metrics, which dimensions, which table, which columns — and that binding
doesn't change from run to run. Freshness does change on every run, so baking a timestamp into a file
that only gets updated when someone edits the deployment would mean the timestamp is stale the moment
it's committed.

Instead, the external pipeline reports freshness after each build completes, by calling:

```
POST /preaggs/{preagg_id}/availability/
```

```json
{
  "catalog": "warehouse",
  "schema": "analytics",
  "table": "views_by_page_daily",
  "valid_through_ts": 20260721
}
```

The `preagg_id` is the id returned when the pre-aggregation is registered (in the `POST
/preaggs/register` response, or from `GET /preaggs/?node_name=<parent>`). `valid_through_ts` is an
integer timestamp in the table's partition/temporal format (e.g. `yyyyMMdd`), not an ISO string.

Until this call has been made at least once, the pre-aggregation exists but has no availability, so
routing won't send queries to it — the same rule that applies to any DJ-materialized pre-aggregation
that hasn't finished its first build.

### Freshness gating

By default, DJ treats a pre-aggregation with any availability at all as eligible: once you've reported
availability once, routing keeps sending queries there even if the table only ever got a partial
backfill, or the pipeline behind it later stops running. Setting `PREAGG_FRESHNESS_GATING=true` makes
DJ check, on every query, that the range the table actually covers contains the range the query asks
for.

The coverage DJ trusts is `min_temporal_partition` and `max_temporal_partition` — the lowest and
highest partition values you report alongside `valid_through_ts`. The check is two-sided, and both
sides matter. A query reaching above the covered range gets silently truncated numbers; a query
reaching below it gets numbers missing everything before the first backfilled partition, which is the
more common failure of the two. Either one falls back to computing from source instead.

What DJ compares against is the time range the query itself asks for, not wall clock. Bounds are read
off the query's filters on the pre-aggregation's temporal partition — `date_id >= 20240101`,
`date_id <= 20250101`, an `=`, or either side of a `BETWEEN`. A query bounded inside the covered range
is served however old the table is, so a report from last March keeps reading the aggregate. A side
the query leaves open places no constraint, so a table holding only the last two years still answers a
query with no lower bound.

If you report no `max_temporal_partition`, DJ falls back to `valid_through_ts` for the upper side.
That's the usual case for a table registered through `/preaggs/register`, which takes only the scalar.
Since `valid_through_ts` is written in a few different encodings in practice, DJ compares it only when
its magnitude is consistent with the bound it's being compared against, and logs a warning rather than
guessing when it isn't.

A query with no upper bound at all implicitly asks for data through the present, and no partition
comparison can settle that on its own. Those queries are allowed through unless you also set
`PREAGG_MAX_STALENESS_SECONDS`, which gives them a wall-clock budget: DJ renders `now - budget` into
the partition's format and requires the covered range to reach it.

Gating only applies to pre-aggregations whose grain includes exactly one temporal partition column —
that's the axis the covered range describes. A pre-aggregation with no temporal dimension, or with two
of them, is never rejected, because there's nothing unambiguous to compare against. Rejection is
silent: the query falls back to source and returns correct (if slower) results, the same as any other
non-match.

### External pre-aggregations are read-only to DJ

Registering a table this way sets its materialization strategy to `external`. DJ will refuse to
materialize or backfill an external pre-aggregation — there's no build SQL for DJ to run, because the
table isn't DJ's to build. Ownership of the table's contents, refresh schedule, and correctness stays
entirely with your external pipeline; DJ's role is limited to routing queries to it when it's a good
match and staying out of its way otherwise.
