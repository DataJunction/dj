---
title: "Developers"
description: "Developers"
lead: ""
draft: false
images: []
weight: 50
---

This section is for people working on DJ itself, or building against its API and client libraries.
It covers getting a full DJ stack running on your machine, the internals worth understanding before
you change them, and the project conventions around releases and docs.

Start with [Running DJ Locally](./running-locally/) — the Docker Compose setup brings up the backend,
the UI, a reflection service, a query service, and a JupyterLab instance with example notebooks, so
you have something to develop against before reading anything else.

## In this section

* [Running DJ Locally](./running-locally/) — get a full stack up with Docker Compose.
* [Clients](./clients/) — the Python and JavaScript client libraries, and how to use them.
* [Authentication](./authentication/) — the authentication schemes DJ supports and the settings that
  enable them.
* [How Metric Requests are Converted to SQL](./how-metric-requests-are-converted-to-sql/) — the
  request-to-SQL pipeline, useful for debugging generated queries or changing the generator.
* [DJ server API specification](./the-datajunction-api-specification/) — the generated reference for
  every server endpoint.
* [Docs Development](./docs-development/) — how this Hugo site is built, tested locally, and deployed.
* [Releasing New Version](./releasing-new-version/) — publishing the server, services, and clients to
  PyPI and NPM.
