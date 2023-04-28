---
title: Introduction
type: docs
bookToc: false
---
# Overview

## Introduction

DataJunction (DJ) is an open source **metrics platform** that allows users to define 
metrics and the data models behind them using **SQL**, serving as a **semantic layer** 
on top of a physical data warehouse. By leveraging this metadata, DJ can enable efficient 
retrieval of metrics data across different dimensions and filters.

![DataJunction](/datajunction-illustration.png)

## How does this work?

At its core, DJ stores metrics and their upstream abstractions as interconnected nodes. 
These nodes can represent a variety of elements, such as tables in a data warehouse 
(**source nodes**), SQL transformation logic (**transform nodes**), dimensions logic,
metrics logic, and even selections of metrics, dimensions, and filters (**cube nodes**). 

By parsing each node's SQL into an AST and through dimensional links between columns, 
DJ can infer a graph of dependencies between nodes, which allows it to find the 
appropriate join paths between nodes to generate queries for metrics.

{{< columns >}}
{{< gradient-text >}}
Define
{{< /gradient-text >}}

**Define Metric Using SQL**

Create and combine DJ nodes to define the steps to calculating
a metric using only SQL.

<--->
{{< gradient-text >}}
Discover
{{< /gradient-text >}}

**Discover New Dimensions**

Find the metrics you're interested in and instantly
discover the dimensions that are common among them.

<--->
{{< gradient-text >}}
Understand
{{< /gradient-text >}}

**Understand Dependencies**

DJ understands the dependencies for metrics across the entire system using a robust metadata layer.
{{< /columns >}}

{{< columns >}}
{{< gradient-text >}}
Build
{{< /gradient-text >}}

**Build Cubes**

Easily build and materialize cubes for sets of metrics and dimensions that power your analytics products.

<--->
{{< gradient-text >}}
Prevent
{{< /gradient-text >}}

**Prevent Broken Metrics**

Develop quickly and let DJ highlight the impact each change will have on all metrics.

<--->
{{< gradient-text >}}
Empower
{{< /gradient-text >}}

**Empower Viz Engineers**

Put the power of combining and grouping metrics into the hands of those building
the presentation layers that drive business decisions.
{{< /columns >}}