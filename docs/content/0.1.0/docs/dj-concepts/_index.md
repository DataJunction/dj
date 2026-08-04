---
weight: 20
title: "DJ Concepts"
---

This section explains how a DJ server works underneath — how your data model is represented, how
queries against it are planned, and what happens when it changes. You can read it straight through,
or dip into individual pages as reference once you've started modelling.

## Pages in this section

The first four pages build on each other and are worth reading in order:

1. [Nodes](./nodes/) — the node types DJ is built from and what they have in common.
2. [The DJ DAG](./the-dj-dag/) — how nodes and their dependencies form the single graph DJ reasons over.
3. [Dimension Discovery](./dimension-discovery/) — dimension links, and how DJ works out which dimensions a metric can be grouped by.
4. [Node Dependencies](./node-dependencies/) — how upstream and downstream relationships are derived from node queries.

The rest stand on their own:

* [DAG Impact Analysis](./dag-impact-analysis/) — finding out what a change to a node would break before you make it.
* [Materialization](./materialization/) — pre-computing cube and node data for low-latency access.
* [Metric Decomposition](./metric-decomposition/) — how complex metrics are broken into pre-aggregatable measures.
* [Query Routing & Aggregate Awareness](./aggregate-awareness/) — how DJ chooses between a materialized dataset and a live computation.
* [Table Reflection](./table-reflection/) — keeping source nodes in step with the external tables behind them.
