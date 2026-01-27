---
weight: 1
title: "Overview"
---

Data modeling in DJ can be done in several ways:
* Using the UI
* Using the [API](../../developers/dj-server-v0.0.1a44)
* Any [supported clients](../dj-clients/)
* Using [YAML files](./yaml)

## Data Modeling Stages

The typical flow for onboarding data models looks like this:
1. Create appropriate [namespaces](../namespaces/) for organization.
1. Register tables as [source nodes](../sources/) in DJ.
2. Create [transform nodes](../transforms/) if any additional, light-weight SQL transformations are necessary.
3. Create [dimension nodes](../dimensions/), depending on what dimensions are needed. These may be simple references to existing source nodes, if the dimensional modeling has already been done outside of DJ.
4. [Link dimensions](../dimension-links) into your source, transform, or dimension nodes to build out DJ's dimensional graph.
5. Create [metric nodes](../metrics/), which are aggregate expressions on one or more fields from a source, transform, or dimension node.
6. Create [cube nodes](../cubes/) for bundling commonly used metrics and dimensions together (often used to facilitate materialization for quick access, like when powering analytical dashboards).

We'll walk through this in more detail in the [Data Modeling Tutorial](#).
