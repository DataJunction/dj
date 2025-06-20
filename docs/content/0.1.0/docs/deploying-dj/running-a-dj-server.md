---
weight: 20
title: Running a DJ Server
---

# Running the core DJ Server

This guide walks through configuring and running a DJ (DataJunction) server.

## Configuration

DJ uses [Pydantic](https://docs.pydantic.dev/latest/) to manage configuration, with support for environment variables (including nested variables using `__` as a delimiter). The `Settings` class controls server behavior, and each setting can be overridden via environment variables or configuration files.

Below is a list of all useful server settings.

## Core Server Settings

| Setting                         | Type              | Default                          | Description |
|---------------------------------|-------------------|----------------------------------|-------------|
| `query_service`            | `str \| None`   | `None`                | Optional external query service URL. |
| `source_node_namespace`    | `str`           | `"source"`            | Namespace for automatically created source nodes. |
| `transpilation_plugins`        | `List[str]`       | `["default", "sqlglot"]`         | Enabled transpilation plugins. |
| `dj_logical_timestamp_format` | `str \| None` | `"${dj_logical_timestamp}"` | Macro for logical timestamp substitution. |
| `secret`                       | `str`             | `"a-fake-secretkey"`             | 128-bit secret for JWT and encryption. |
| `default_catalog_id` | `int`        | `0`     | Default catalog ID for new nodes. |
| `index_cache_expire` | `int`        | `60`    | Interval in seconds before cache expiration for indexes. |
| `node_list_max`      | `int`        | `10000` | Max number of nodes returned in a list request. |

### Database Settings

DJ supports separate database connections for read and write operations. If no `reader_db` is set, it defaults to `writer_db`.

| Setting                 | Type      | Default               | Description |
|------------------------|-----------|-----------------------|-------------|
| `uri`                  | `str`     | `"postgresql+psycopg://dj:dj@postgres_metadata:5432/dj"` | SQLAlchemy-compatible DB URI. |
| `pool_size`            | `int`     | `20`                  | Connection pool size. |
| `max_overflow`         | `int`     | `20`                  | Max overflow connections. |
| `pool_timeout`         | `int`     | `10`                  | Timeout for acquiring a connection (in seconds). |
| `connect_timeout`      | `int`     | `5`                   | Timeout for initial DB connection (in seconds). |
| `pool_pre_ping`        | `bool`    | `True`                | Enable pre-ping checks. |
| `echo`                 | `bool`    | `False`               | Enable SQLAlchemy echo mode for debugging. |
| `keepalives`           | `int`     | `1`                   | Enable TCP keepalives. |
| `keepalives_idle`      | `int`     | `30`                  | Idle time before sending keepalives. |
| `keepalives_interval`  | `int`     | `10`                  | Interval between keepalives. |
| `keepalives_count`     | `int`     | `5`                   | Number of failed probes before dropping. |

## Environment Variable Example

Here's an example `.env` file. Note that environment variables can be set using
double underscores for nested fields:
```bash
QUERY_SERVICE=http://djqs:8001
SECRET=a-fake-secretkey
NODE_LIST_MAX=10000

# Writer DB (required)
WRITER_DB__URI=postgresql+psycopg://dj:dj@postgres_metadata:5432/dj
WRITER_DB__POOL_SIZE=20
WRITER_DB__MAX_OVERFLOW=20
WRITER_DB__POOL_TIMEOUT=10
WRITER_DB__CONNECT_TIMEOUT=5
WRITER_DB__POOL_PRE_PING=true
WRITER_DB__ECHO=false
WRITER_DB__KEEPALIVES=1
WRITER_DB__KEEPALIVES_IDLE=30
WRITER_DB__KEEPALIVES_INTERVAL=10
WRITER_DB__KEEPALIVES_COUNT=5

# Reader DB (optional)
READER_DB__URI=postgresql+psycopg://dj:dj@postgres_metadata:5432/dj
READER_DB__POOL_SIZE=10
READER_DB__MAX_OVERFLOW=10
READER_DB__POOL_TIMEOUT=5
READER_DB__CONNECT_TIMEOUT=5
READER_DB__POOL_PRE_PING=true
READER_DB__ECHO=false
READER_DB__KEEPALIVES=1
READER_DB__KEEPALIVES_IDLE=30
READER_DB__KEEPALIVES_INTERVAL=10
READER_DB__KEEPALIVES_COUNT=5
```
