---
weight: 1
title: "Getting Started"
---

If you're new to DataJunction, this is where to begin. The pages here cover what DJ is, how to
connect to a server, and how to get metrics, SQL, and data back out of one — enough to be useful
before you start modelling anything yourself.

Read [Introduction](./introduction/) first, then pick whichever interface you plan to use.

## Pages in this section

* [Introduction](./introduction/) — what DataJunction is, the problems it solves, and where to go next depending on your role. Start here.
* [DataJunction + Trino Quickstart](./dj-and-trino/) — stand up a local DJ instance over a dockerized Trino and work through registering tables and defining metrics end to end.
* [Using the DJ CLI](./using-the-cli/) — installing, configuring, and authenticating the `dj` command line client.
* [Using DJ with AI Assistants](./ai-assistants/) — connecting Claude and other assistants to your DJ instance through MCP.
* [Listing Metrics](./listing-metrics/) — discovering which metrics a server exposes.
* [Requesting SQL](./requesting-sql/) — asking DJ to generate SQL for metrics, dimensions, and filters.
* [Requesting Data](./requesting-data/) — running those queries and getting results back.

When you're ready to define your own nodes, continue on to [Data Modeling](../data-modeling/).
