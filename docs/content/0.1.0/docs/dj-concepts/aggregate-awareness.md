---
weight: 86
title: "Query Routing & Aggregate Awareness"
---

Query routing is how DataJunction decides where a metric query reads from: a pre-aggregated
(materialized) dataset, or a live computation from source tables. When a suitable materialization
exists, DJ routes the query to it automatically — you don't ask for a specific cube or
pre-aggregation, you just request metrics and dimensions and DJ picks the best available source.
This automatic selection of a pre-aggregated source is what's meant by **aggregate awareness**.

## How routing chooses a source

DJ routes by **per-query matching**. On each request it gathers the candidate materializations for
the requested metrics and checks, in the moment, whether any of them can satisfy the query, using the
rules in the next section. There is no precomputed query-to-table mapping it consults; the match is
decided fresh each time against whatever materializations currently exist.

What you manage is the set of **materializations** themselves — the cubes and pre-aggregations that
exist for your nodes. Routing then matches against whatever is currently available; a new
materialization becomes usable as soon as it has data.

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

Measures are matched by the **expression hash** of the decomposed measure, not by name — two metrics
that decompose to the same underlying measure expression share a pre-aggregation. As with cubes, when
several pre-aggregations qualify, DJ chooses the **smallest grain** that covers the request.

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
- Measures shared across metrics (same decomposed expression) are matched by hash, so one
  pre-aggregation can back several metrics.

For how metrics are broken into the additive measures that pre-aggregations store, see
[Metric Decomposition](../metric-decomposition/); for how materializations are configured, see
[Materialization](../materialization/).

## Registering externally-built pre-aggregations

Everything above assumes DJ built the pre-aggregation itself. But the pre-aggregation matcher doesn't
actually care who built the table — only that its shape and grain are known. That means you can hand DJ
a table an outside pipeline already built — a nightly Spark job, a dbt model, an ETL DAG someone on
your data platform team owns — and have query routing treat it exactly like a pre-aggregation DJ
materialized on its own. DJ never runs, refreshes, or owns the table; it only learns enough about the
table's shape to route matching queries to it. This is DJ's equivalent of Oracle's
`MATERIALIZED VIEW ... ON PREBUILT TABLE` for a semantic layer: you bring the data, DJ brings the query
routing.

Because the matching rules are unchanged, the only new concept is *how* the pre-aggregation comes into
existence — instead of DJ generating and running the build SQL, you tell DJ where an already-built
table lives and how its columns map to measures.

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

### Registering a table

There are two ways to tell DJ about an externally-built table: a one-off REST call, or a declarative
YAML file that lives alongside your other node definitions in a deployment. The YAML path is recommended
for anything you intend to keep around, since it's versioned with the rest of your semantic model and
gets reconciled on every deploy.

#### REST: `POST /preaggs/register`

Send DJ the metrics and dimensions the table covers, where the table lives, and which physical column
backs each measure metric:

```json
{
  "metrics": ["default.view_secs", "default.session_count", "default.view_rate"],
  "dimensions": ["default.page_d.page_id"],
  "table": {
    "catalog": "warehouse",
    "schema": "analytics",
    "table": "views_by_page_daily"
  },
  "measure_columns": {
    "default.view_secs": "view_secs_sum",
    "default.session_count": "session_cnt"
  }
}
```

Notice that `view_rate`, a ratio metric, is listed under `metrics` but doesn't appear in
`measure_columns` — it doesn't need a column, because it's derived from `view_secs` and
`session_count`, which are the two measures that do have columns.

On registration DJ decomposes every metric you listed, then validates the binding: it confirms each key
in `measure_columns` really is a measure, checks (via query-service introspection) that every column you
named actually exists in the table **and is type-compatible with the measure it backs** — a numeric
`SUM` can't bind to a string column, for instance (the check is category-level, so `int` vs `bigint`
vs `decimal` are all fine) — and confirms that every measure any of your metrics decomposes into is
covered by some column. If all of that checks out, DJ records the pre-aggregation. If you also pass a
`valid_through_ts`, DJ marks it available immediately so routing can start using it right away.

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
metrics:
  - ${prefix}view_rate
dimensions:
  - ${prefix}page_d.page_id
catalog: warehouse
schema: analytics
table: views_by_page_daily
measure_columns:
  ${prefix}view_secs: view_secs_sum
  ${prefix}session_count: session_cnt
```

`view_rate` is listed as the metric you care about querying, but the column mapping is still declared
against its underlying measures — DJ resolves `view_rate`'s dependency on `view_secs` and
`session_count`, sees both are covered by columns in `measure_columns`, and considers `view_rate`
covered as a result.

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

### External pre-aggregations are read-only to DJ

Registering a table this way sets its materialization strategy to `external`. DJ will refuse to
materialize or backfill an external pre-aggregation — there's no build SQL for DJ to run, because the
table isn't DJ's to build. Ownership of the table's contents, refresh schedule, and correctness stays
entirely with your external pipeline; DJ's role is limited to routing queries to it when it's a good
match and staying out of its way otherwise.

### Limitations

External pre-aggregation registration covers the common case well, but there are some edges worth
knowing about:

- **Only measure columns are validated.** DJ checks that each column in `measure_columns` exists and
  backs a real measure, but it currently trusts that your grain/dimension columns already match DJ's
  expected output names. Naming your dimension columns the same way DJ would (as if it had built the
  table itself) avoids surprises.
- **Non-additive measures have a narrower routing window.** A column backing `COUNT(DISTINCT x)` can
  only serve queries at the exact grain it was built at, since distinct counts can't be rolled up to a
  coarser grain from a pre-aggregated table without the underlying row-level values.
- **The table has to fully cover what you register.** Every component measure that your registered
  metrics decompose into needs a column, and that column's values need to actually correspond to the
  expression in the metric's definition — DJ doesn't verify that a column's *data* matches its claimed
  aggregation, only that the column exists.
- **Filtered or partial tables aren't supported yet.** A table that only covers a subset of rows (e.g.
  a single region or a filtered cohort) can't be registered as a general-purpose pre-aggregation for the
  unfiltered metric.
- **Cross-fact metrics aren't supported for registration yet.** Registration assumes all the metrics and
  measures you're binding trace back to a single parent node, the same restriction pre-aggregation
  matching has generally.
