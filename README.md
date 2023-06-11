# DataJunction

## Introduction

DataJunction (DJ) is an open source **metrics platform** that allows users to define
metrics and the data models behind them using **SQL**, serving as a **semantic layer**
on top of a physical data warehouse. By leveraging this metadata, DJ can enable efficient
retrieval of metrics data across different dimensions and filters.

![DataJunction](docs/static/datajunction-illustration.png)

## How does this work?

At its core, DJ stores metrics and their upstream abstractions as interconnected nodes.
These nodes can represent a variety of elements, such as tables in a data warehouse
(**source nodes**), SQL transformation logic (**transform nodes**), dimensions logic,
metrics logic, and even selections of metrics, dimensions, and filters (**cube nodes**).

By parsing each node's SQL into an AST and through dimensional links between columns,
DJ can infer a graph of dependencies between nodes, which allows it to find the
appropriate join paths between nodes to generate queries for metrics.

## Getting started

While all the functionality above currently works, DJ is still not ready for production use. Only a very small number of functions are supported, and we are still working towards a 0.1 release. If you are interested in helping take a look at the `issues marked with the "good first issue" label <https://github.com/DataJunction/dj/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`_.

### DataJunction Demo

This repo includes a DJ demo setup that launches and connects all of the DJ services as well as a jupyter lab server with example notebooks.

Clone this repository as well as the repositories for the other services.

```sh
git clone https://github.com/DataJunction/dj-demo.git
git clone https://github.com/DataJunction/dj.git
git clone https://github.com/DataJunction/dj-ui.git
git clone https://github.com/DataJunction/djqs.git
git clone https://github.com/DataJunction/djrs.git
git clone https://github.com/DataJunction/djms.git  # Optional
```

Change into the `dj-demo` directory and start up the docker environment.

```sh
cd dj-demo
docker compose up
```

To run the demo notebook, go to [http://localhost:8888/lab/](http://localhost:8888/lab/) and access it under
`/notebooks/Modeling the Roads Example Database.ipynb`. In the same directory, you can find other example notebooks.

**note:** The service urls are configured in the `.env` file. You can modify them to connect to remote services.
