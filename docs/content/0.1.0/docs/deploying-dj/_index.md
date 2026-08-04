---
title: "Deploying DJ"
description: ""
lead: ""
images: []
weight: 30
---

A DJ deployment is not a single process. At minimum it is a FastAPI server and a Postgres database,
but most real deployments also add a query service, a web UI, and a background reflection worker.
This section is for operators standing that up and keeping it running.

Start with [The Components of a DJ Deployment](./the-components-of-a-dj-deployment/), which maps out
what each piece is responsible for, how the pieces find each other, and which ones you can skip.
Once you have that picture, the remaining pages go deeper on individual components.

## In this section

* [Overview](./overview/) — a diagram of a full deployment and the services in it.
* [The Components of a DJ Deployment](./the-components-of-a-dj-deployment/) — the orientation page;
  read this first.
* [Running a DJ Server](./running-a-dj-server/) — configuring and running the core server, including
  the full table of settings.
* [Query Service](./query-service/) — the service that submits generated SQL to your data warehouse.
* [Reflection Service](./reflection-service/) — the background worker that keeps table metadata in
  sync with the warehouse.
* [SQL Plugins](./sql-plugins/) — adding transpilation support for a SQL dialect DJ does not ship
  with.
* [Caching](./caching/) — the default in-memory cache and how to swap in your own.
* [Notifications](./notifications/) — the default notification dependency and how to replace it.
