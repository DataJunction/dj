---
title: "Introduction"
draft: false
images: []
menu:
  docs:
    parent: ""
weight: 700
toc: true
---

## What is it?

DataJunction (DJ) is an open source **metrics platform** that allows users to define metrics
and the data models behind them using **SQL**, serving as a **semantic layer** on top of a physical
data warehouse. By leveraging this metadata, DJ can enable efficient retrieval of metrics data
across different dimensions and filters.

## How does this work?

<img src="/images/dj-landing.png" alt="DJ Landing" style="padding-bottom: 1rem"/>

At its core, DJ stores metrics and their upstream abstractions as interconnected nodes.
These nodes can represent a variety of elements, such as tables in a data warehouse (source
nodes), SQL transformation logic (transform nodes), dimensions logic, metrics logic, and
even selections of metrics, dimensions, and filters (cube nodes).

By parsing each node’s SQL into an AST and through dimensional links between columns, DJ
can infer a graph of dependencies between nodes, which allows it to find the appropriate
join paths between nodes to generate queries for metrics.
