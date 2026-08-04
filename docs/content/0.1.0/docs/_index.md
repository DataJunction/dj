---
title : "Docs"
description: "DJ Docs"
lead: ""
date: 2020-10-06T08:48:23+00:00
lastmod: 2020-10-06T08:48:23+00:00
draft: false
images: []
---

DataJunction (DJ) is an open source metrics platform. You define metrics and the data models behind
them in SQL, and DJ acts as a semantic layer over your physical data warehouse, working out how to
retrieve those metrics across whatever dimensions and filters you ask for.

If this is your first time here, start with [Introduction](./getting-started/introduction/), which
explains what DJ does and how the pieces fit together.

## Where to go next

* [Getting Started](./getting-started/) — for readers new to DJ. What DJ is, and how to list metrics,
  request SQL, and request data from a running server.
* [Data Modeling](./data-modeling/) — for data practitioners defining metrics. How to create
  namespaces, sources, transforms, dimensions, metrics, and cubes, and how to link dimensions
  together into DJ's dimensional graph.
* [DJ Concepts](./dj-concepts/) — for anyone who wants to understand the machinery. The DAG, node
  dependencies, dimension discovery, metric decomposition, materialization, and aggregate awareness.
* [Deploying DJ](./deploying-dj/) — for operators standing up their own DJ deployment. The services
  involved, how to configure them, and which ones are optional.
* [Developers](./developers/) — for people contributing to DJ itself, or building against its API
  and clients.
