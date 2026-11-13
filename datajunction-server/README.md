# DataJunction

## Introduction

DataJunction (DJ) is an open source **metrics platform** that allows users to define
metrics and the data models behind them using **SQL**, serving as a **semantic layer**
on top of a physical data warehouse. By leveraging this metadata, DJ can enable efficient
retrieval of metrics data across different dimensions and filters.

![DataJunction](docs/static/datajunction-illustration.png)

## Getting Started

To launch the DataJunction UI with a minimal DataJunction backend, start the default docker compose environment.

```sh
docker compose up
```

If you'd like to launch the full suite of services, including open-source implementations of the DataJunction query service and
DataJunction reflection service specifications, use the `demo` profile.

```sh
docker compose --profile demo up
```

DJUI: [http://localhost:3000/](http://localhost:3000/)
DJ Swagger Docs: [http://localhost:8000/docs](http://localhost:8000/docs)
DJQS Swagger Docs: [http://localhost:8001/docs](http://localhost:8001/docs)
Jaeger UI: [http://localhost:16686/search](http://localhost:16686/search)
Jupyter Lab: [http://localhost:8888](http://localhost:8888)

## How does this work?

At its core, DJ stores metrics and their upstream abstractions as interconnected nodes.
These nodes can represent a variety of elements, such as tables in a data warehouse
(**source nodes**), SQL transformation logic (**transform nodes**), dimensions logic,
metrics logic, and even selections of metrics, dimensions, and filters (**cube nodes**).

By parsing each node's SQL into an AST and through dimensional links between columns,
DJ can infer a graph of dependencies between nodes, which allows it to find the
appropriate join paths between nodes to generate queries for metrics.

## Semantic views and Explore

The semantic layer API exposes cubes as views and adds an **Explore** view at
every discovery level. `POST /semantic/views` accepts either hierarchical
`path` browsing or `runtime_configuration` selectors. For example:

```json
{"path": ["sales", "main"]}
```

The response includes a view named `@explore:sales:main` with display name
`Explore ( sales / main )`. Namespace-only views use `@explore:sales`; the global
view uses `@explore` and display name `Explore`. The `uid` equals the scoped name.
These reserved names cannot collide with a cube's fully qualified DJ node name.

Hierarchy components use UTF-8 URL escaping with `~HH` in place of `%HH`, keeping
each name in one URL path segment even after URL decoding. For example,
`feature/foo` becomes `feature~2Ffoo`. Literal `~`, `%`, and `:` become `~7E`,
`~25`, and `~3A`, so escaped and literal branch names stay distinct.

Use `/semantic/views/{name}` with the **exact name returned by discovery** for
view details and append `/compatible-metrics`,
`/compatible-dimensions`, `/sql`, `/query`, `/row-count`, or `/values` for the
usual semantic operations. No scope needs to be forwarded separately, so generic
Semantic Layer clients, including the Superset extension, can use these views
without modifications. For example, the compatibility body can be:

```json
{
  "additional_configuration": {},
  "selected_metrics": ["sales.main.revenue"],
  "selected_dimensions": []
}
```

The scope is decoded and authorized on every request. Invalid names return 404;
conflicting namespace or branch options in `additional_configuration` return 400.
The name is an identity, not an authorization grant.

With neither discovery selector, Explore includes readable active non-Git
namespaces and only the literal `main` branch of Git repositories. A plain
namespace includes only its direct elements. A Git root's Explore view also
defaults to `main`, including matching nested namespaces and explicitly linked
branch namespaces outside the root's name prefix. This does not follow a
repository's configured default branch: a repository without `main` contributes
no Git elements to broader Explore views. Select a branch explicitly to explore
another branch, such as `@explore:sales:feature~2Frevenue`. Branch selectors and
cube discovery still expose other branches. Independent nested repositories are
excluded from a Git root's Explore view. Branch names use their actual Git
spelling, such as `feature/revenue`. A branch requires a Git root namespace.

Explore exposes valid, active metrics and dimension attributes the caller can
read in the selected scope, including tagged local dimensions and role-qualified
references. It does not require a cube. Empty selections return every available
element. Selected metrics restrict dimensions to their common reachable
attributes; selected dimensions restrict metrics to those supporting all of
them. Dimension-only selections also narrow the remaining dimensions to
compatible metric domains or attributes of the same dimension node. Unknown,
inaccessible, or out-of-scope selections are rejected.

Explore metric and dimension `name` values use their fully qualified DJ IDs,
including role suffixes, so columns with the same leaf name remain distinct.
Query result schemas and row keys use these same names, independent of physical
SQL aliases. Human-readable labels remain available in column metadata.

Explore queries use live source definitions rather than a cube's stored filters
or materialization. Dimension-only queries use the existing single-node domain
query behavior; select a metric to combine attributes across dimension nodes.
Existing cube views retain their fixed element sets, filters, and compatibility.

## AI Integration

DataJunction provides an MCP (Model Context Protocol) client that enables AI assistants like Claude to interact with your semantic layer.

The MCP client is part of the [DataJunction Python client package](../datajunction-clients/python/).

For installation and setup, see the [MCP documentation in the client package](../datajunction-clients/python/README_MCP.md).

## Documentation

For more detailed documentation, visit [datajunction.io](https://datajunction.io).
