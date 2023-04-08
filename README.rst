==========================
DataJunction Query Service
==========================

This repository (DJQS) is an open source implementation of a `DataJunction <https://github.com/DataJunction/dj>`_
query service. It allows you to create catalogs and engines that represent sqlalchemy connections. Configuring
a DJ server to use a DJQS server allows DJ to query any of the database technologies supported by sqlalchemy.

==========
Quickstart
==========

To get started, clone this repo and start up the docker compose environment.

.. code-block::

    git clone https://github.com/DataJunction/djqs
    cd djqs
    docker compose up

Creating Catalogs
=================

Catalogs can be created using the :code:`POST /catalogs/` endpoint.

.. code-block:: sh

    curl -X 'POST' \
      'http://localhost:8001/catalogs/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "djdb"
    }'

Creating Engines
================

Engines can be created using the :code:`POST /engines/` endpoint.

.. code-block:: sh

    curl -X 'POST' \
      'http://localhost:8001/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "sqlalchemy-postgresql",
      "version": "15.2",
      "uri": "postgresql://dj:dj@postgres-roads:5432/djdb"
    }'

Engines can be attached to existing catalogs using the :code:`POST /catalogs/{name}/engines/` endpoint.

.. code-block:: sh

    curl -X 'POST' \
      'http://localhost:8001/catalogs/djdb/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '[
      {
        "name": "sqlalchemy-postgresql",
        "version": "15.2"
      }
    ]'

Executing Queries
=================

Queries can be submitted to DJQS for a specified catalog and engine.

.. code-block:: sh

    curl -X 'POST' \
      'http://localhost:8001/queries/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "catalog_name": "djdb",
      "engine_name": "sqlalchemy-postgresql",
      "engine_version": "15.2",
      "submitted_query": "SELECT * from roads.repair_orders",
      "async_": false
    }'

Async queries can be submitted as well.

.. code-block:: sh

    curl -X 'POST' \
      'http://localhost:8001/queries/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "catalog_name": "djdb",
      "engine_name": "sqlalchemy-postgresql",
      "engine_version": "15.2",
      "submitted_query": "SELECT * from roads.repair_orders",
      "async_": true
    }'

*response*

.. code-block:: json

    {
      "catalog_name": "djdb",
      "engine_name": "sqlalchemy-postgresql",
      "engine_version": "15.2",
      "id": "<QUERY ID HERE>",
      "submitted_query": "SELECT * from roads.repair_orders",
      "executed_query": null,
      "scheduled": null,
      "started": null,
      "finished": null,
      "state": "ACCEPTED",
      "progress": 0,
      "results": [],
      "next": null,
      "previous": null,
      "errors": []
    }

The query id provided in the response can then be used to check the status of the running query and get the results
once it's completed.

.. code-block:: sh

    curl -X 'GET' \
      'http://localhost:8001/queries/<QUERY ID HERE>/' \
      -H 'accept: application/json'

*response*

.. code-block:: json

    {
      "catalog_name": "djdb",
      "engine_name": "sqlalchemy-postgresql",
      "engine_version": "15.2",
      "id": "$QUERY_ID",
      "submitted_query": "SELECT * from roads.repair_orders",
      "executed_query": "SELECT * from roads.repair_orders",
      "scheduled": "2023-02-28T07:27:55.367162",
      "started": "2023-02-28T07:27:55.367387",
      "finished": "2023-02-28T07:27:55.502412",
      "state": "FINISHED",
      "progress": 1,
      "results": [
        {
          "sql": "SELECT * from roads.repair_orders",
          "columns": [...],
          "rows": [...],
          "row_count": 25
        }
      ],
      "next": null,
      "previous": null,
      "errors": []
    }

Reflection
==========

If running a [reflection service](https://github.com/DataJunction/djrs), that service can leverage the
:code:`POST /table/{table}/columns/` endpoint of DJQS to get column names and types for a given table.

.. code-block:: sh

    curl -X 'GET' \
      'http://localhost:8001/table/djdb.roads.repair_orders/columns/?engine=sqlalchemy-postgresql&engine_version=15.2' \
      -H 'accept: application/json'

*response*

.. code-block:: json

    {
      "name": "djdb.roads.repair_orders",
      "columns": [
        {
          "name": "repair_order_id",
          "type": "INT"
        },
        {
          "name": "municipality_id",
          "type": "STR"
        },
        {
          "name": "hard_hat_id",
          "type": "INT"
        },
        {
          "name": "order_date",
          "type": "DATE"
        },
        {
          "name": "required_date",
          "type": "DATE"
        },
        {
          "name": "dispatched_date",
          "type": "DATE"
        },
        {
          "name": "dispatcher_id",
          "type": "INT"
        }
      ]
    }

======
DuckDB
======

DJQS includes an example of using DuckDB as an engine and it comes preloaded with the roads example database.

Create a :code:`djduckdb` catalog and a :code:`duckdb` engine.

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/catalogs/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "djduckdb"
    }'

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "duckdb",
      "version": "0.7.1",
      "uri": "duckdb://local[*]"
    }'

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/catalogs/djduckdb/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '[
      {
        "name": "duckdb",
        "version": "0.7.1"
      }
    ]'

Now you can submit DuckDB SQL queries.

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/queries/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "catalog_name": "djduckdb",
      "engine_name": "duckdb",
      "engine_version": "0.7.1",
      "submitted_query": "SELECT * FROM roads.us_states LIMIT 10",
      "async_": false
    }'

=====
Spark
=====

DJQS includes an example of using Spark as an engine. To try it, start up the docker compose environment and then
load the example roads database into Spark.

.. code-block::

    docker exec -it djqs /bin/bash -c "python /code/docker/spark_load_roads.py"

Next, create a :code:`djspark` catalog and a :code:`spark` engine.

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/catalogs/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "djspark"
    }'

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "spark",
      "version": "3.3.2",
      "uri": "spark://local[*]"
    }'

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/catalogs/djspark/engines/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '[
      {
        "name": "spark",
        "version": "3.3.2"
      }
    ]'

Now you can submit Spark SQL queries.

.. code-block::

    curl -X 'POST' \
      'http://localhost:8001/queries/' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "catalog_name": "djspark",
      "engine_name": "spark",
      "engine_version": "3.3.2",
      "submitted_query": "SELECT * FROM roads.us_states LIMIT 10",
      "async_": false
    }'
