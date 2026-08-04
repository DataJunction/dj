---
weight: 10
title: The Components of a DJ Deployment
---

A DJ deployment is not a single process. At minimum it is a FastAPI server and a Postgres database, but most
real deployments add a query service, a web UI, and a background reflection worker. This page is a map of
those pieces: what each one is responsible for, how it finds the others, and which of them you can skip. The
[Overview](../overview) has a diagram of the same picture, and each component has its own page linked below.

## The core server

The core server is the DJ API — the FastAPI application at `datajunction_server.api.main:app`, served with
`uvicorn` in the reference `docker-compose.yml`. It is the only component that owns semantic metadata, and
everything else in the deployment either talks to it or is talked to by it.

The core server owns:

* **The node graph.** Sources, transforms, dimensions, metrics, and cubes, along with their versions, their
  columns, and the dimension links between them.
* **SQL generation.** Parsing node queries, resolving requested metrics and dimensions against the DAG, and
  emitting dialect-specific SQL. Dialect support comes from transpilation plugins, configured with
  `transpilation_plugins` and described in [SQL Plugins](../sql-plugins).
* **Validation and status.** Deciding whether a node is valid given its parents, and propagating invalidity
  downstream when an upstream column changes.
* **Authentication and authorization.** JWT issuance signed with `secret`, optional GitHub and Google OAuth,
  and an authorization layer selected by `authorization_provider` with a `default_access_policy` of
  `permissive` or `restrictive`. See [Authentication](../../developers/authentication).
* **Materialization metadata.** The definition of what should be materialized and on what schedule. The core
  server does not run those workflows itself; it hands them to the query service.

The core server is required. Nothing else in the deployment works without it.

## The metadata database

DJ stores all of its metadata in a Postgres database. This is the durable state of the deployment — losing it
loses your semantic layer, and it is the thing to back up.

Connection settings live under `writer_db`, a nested `DatabaseConfig` covering the SQLAlchemy `uri`,
`pool_size`, timeouts, and TCP keepalives. A deployment with a read replica can point `reader_db` at it; when
`reader_db` is unset it falls back to `writer_db`, so a single-instance Postgres needs only the writer
configured. The full table of database settings is in [Running a DJ Server](../running-a-dj-server).

Schema changes are managed with [Alembic](https://alembic.sqlalchemy.org/). The migration environment lives
inside the server package, at `datajunction_server/alembic/` with its own `alembic.ini`, and revisions are in
`alembic/versions/`. Migrations run as a step separate from the server itself — the `db-migration` service in
`docker-compose.yml` runs

```sh
alembic -x uri="postgresql+psycopg://dj:dj@postgres_metadata:5432/dj" upgrade head
```

from the `datajunction_server` directory, and the API container is not allowed to start until it completes.
The `-x uri=` argument overrides the target database; without it, Alembic falls back to the `DATABASE_URI`
environment variable. That is a different variable from the server's own `WRITER_DB__URI`, so wiring up a real
deployment means setting the database location for the migration step as well as for the server.

A fresh database also needs a small amount of seeding — the compose file's `db-seed` step creates the initial
`dj` user — before you can log in.

## The query service

The core server generates SQL but never connects to your warehouse to run it. That job belongs to the query
service, a separate and pluggable component. DJ talks to it through the `BaseQueryServiceClient` interface in
`datajunction_server/query_clients/base.py`, and delegates:

* **Table reflection.** `get_columns_for_table` and `get_columns_for_tables_batch`, used when registering a
  source table and when refreshing an existing one.
* **Query execution.** `submit_query` and `get_query`, which is how requests for data are actually answered.
* **View creation.** `create_view`, used for cube views.
* **Materialization.** `materialize`, `materialize_cube`, `materialize_preagg`, backfills, and deactivation.
  DJ describes the workflow; the query service is what registers it with a scheduler.

Which implementation gets used is decided by `get_query_service_client` in `datajunction_server/utils.py`, and
there are two ways to configure it:

* `query_service`, a URL, is the original setting. It selects the HTTP client, which speaks to a remote
  service over the DJ query service API. The reference implementation of that API is
  [DJQS](https://github.com/DataJunction/djqs), included in this repository under `datajunction-query/` and
  run as the `djqs` service in `docker-compose.yml`. This is the right shape at production scale.
* `query_client`, a nested `QueryClientConfig`, is the newer and more general form. Set `QUERY_CLIENT__TYPE`
  to `http`, `snowflake`, or `bigquery` and supply connection parameters under `QUERY_CLIENT__CONNECTION__*`.
  The `snowflake` and `bigquery` clients connect DJ directly to the warehouse with no separate service in
  between, which suits smaller deployments and demos; they require the corresponding extras
  (`datajunction-server[snowflake]`, `datajunction-server[bigquery]`).

The query service is optional. If neither setting is configured, `get_query_service_client` returns `None` and
DJ still models, validates, and generates SQL — you simply cannot execute that SQL, reflect tables, or
materialize through DJ. See [Query Service](../query-service) for more.

## The reflection service

External warehouse tables change without telling DJ. The reflection service is what keeps source nodes honest
about them: a Celery worker plus a beat scheduler that polls the core server for nodes with associated tables,
asks the query service for each table's current schema, available partitions, and valid-through timestamp, and
writes the results back to the core server. Its implementation lives in `datajunction-reflection/` and runs as
the `djrs-worker` and `djrs-beat` services in `docker-compose.yml`.

It configures itself rather than being configured through the core server's settings: `core_service`,
`query_service`, `celery_broker`, `celery_results_backend`, and `polling_interval` (default 3600 seconds), all
in `datajunction_reflection/config.py`. Its Celery broker and result backend are the Redis instance that
compose runs as `djrs-redis`.

Reflection is optional, and it depends on the query service — it has nothing to ask if there is no query
service to answer. Without it, source node schemas only change when something explicitly refreshes them. See
[Reflection Service](../reflection-service) and the concept page on
[Table Reflection](../../dj-concepts/table-reflection).

## The UI

`datajunction-ui/` is a React application for browsing and editing the semantic layer, and it is a pure client
of the core API. Point it at the server with `REACT_APP_DJ_URL`, and add its origin to the server's
`cors_origin_whitelist`; if you use OAuth, `frontend_host` is where the server redirects back to after login.
The UI is optional — the API and the Python and JavaScript clients are all usable without it.

## Caching

Caching in the core server is in-process rather than a separate service. `get_cache` in
`datajunction_server/internal/caching/cachelib_cache.py` is a FastAPI dependency returning a `SimpleCache`
from `cachelib`, shared across requests within a single worker, and it backs the expensive read paths: node
and metric listings, dimension DAG computation, and generated SQL. Two settings control expiry —
`index_cache_expire` (seconds, default 60) for index-style caches and `query_cache_timeout` for cached SQL —
with `query_cache_max_concurrent_refreshes` capping how many refresh-ahead rebuilds run at once. Because the
cache lives in the process, a multi-worker or multi-replica deployment gets one cache per worker; replacing it
with something shared means overriding the dependency, which [Caching](../caching) walks through.

Two Redis-shaped settings on the core server's `Settings` deserve a warning. `redis_cache` builds a `cachelib`
`RedisCache` on the `settings.cache` property, and `results_backend` defaults to a `FileSystemCache` under
`/tmp/dj`, but neither is read by any request path in the server today — they are vestigial, and setting them
will not move the caching described above onto Redis. The Redis in `docker-compose.yml` belongs to the
reflection service's Celery, not to the core server's cache.

## Putting it together

| Component | Required? | Configured by |
|---|---|---|
| Core server | Yes | The `Settings` class; see [Running a DJ Server](../running-a-dj-server) |
| Postgres metadata DB | Yes | `writer_db`, optionally `reader_db`; Alembic via `-x uri=` or `DATABASE_URI` |
| Query service | Optional; needed to execute SQL, reflect tables, or materialize | `query_service` or `query_client` |
| Reflection service | Optional; requires a query service | Its own settings: `core_service`, `query_service`, `celery_broker` |
| UI | Optional | `REACT_APP_DJ_URL`, plus `cors_origin_whitelist` and `frontend_host` on the server |

Every setting named above is documented with its type and default in
[Running a DJ Server](../running-a-dj-server), which is the next page to read. From there,
[Query Service](../query-service) and [Reflection Service](../reflection-service) cover the two satellite
services, [SQL Plugins](../sql-plugins) covers adding a dialect, and [Caching](../caching) and
[Notifications](../notifications) cover the two dependencies you are most likely to want to swap for your own
implementation.
