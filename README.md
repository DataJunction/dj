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

## How does this work?

At its core, DJ stores metrics and their upstream abstractions as interconnected nodes.
These nodes can represent a variety of elements, such as tables in a data warehouse
(**source nodes**), SQL transformation logic (**transform nodes**), dimensions logic,
metrics logic, and even selections of metrics, dimensions, and filters (**cube nodes**).

By parsing each node's SQL into an AST and through dimensional links between columns,
DJ can infer a graph of dependencies between nodes, which allows it to find the
appropriate join paths between nodes to generate queries for metrics.
