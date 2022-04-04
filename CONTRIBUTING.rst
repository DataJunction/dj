============
Contributing
============

Pre-requisites
==============

DataJunction (DJ) is currently supported in Python 3.8, 3.9, and 3.10. It's recommended to use ``pyenv`` to create a virtual environment called "datajunction", so you can use the included ``Makefile``:

.. code-block:: bash

    $ pyenv virtualenv 3.8 datajunctiona  # or 3.9/3.10

Then you can just run ``make test`` to install all the dependencies.

DJ relies heavily on these libraries:

- `sqloxide <https://pypi.org/project/sqloxide/>`_, for SQL parsing.
- `SQLAlchemy <https://www.sqlalchemy.org/>`_ for running queries and fetching table metadata.
- `SQLModel <https://sqlmodel.tiangolo.com/>`_ for defining models.
- `FastAPI <https://fastapi.tiangolo.com/>`_ for APIs.

The SQLModel documentation is a good start, since it covers the basics of SQLAlchemy and FastAPI as well.

Running the examples
====================

The repository comes with a small set of examples in the ``examples/configs/`` directory, with 3 nodes (``core.comments``, ``core.users``, and ``core.num_comments``) built from 3 databases (Postgres, Druid, Google Sheets).

Running ``docker compose up`` will start a Druid cluster, a Postgres database, and the DJ service (together with Celery workers for running async queries and Redis for caching and storing results).

You can check that everything is working by querying the list of available databasesi (install `jq <https://stedolan.github.io/jq/>`_ if you don't have it):

.. code-block:: bash

    % curl http://localhost:8000/databases/ | jq
    [
      {
        "id": 7,
        "description": "An Apache Druid database",
        "read_only": true,
        "async_": false,
        "cost": 1,
        "created_at": "2022-01-17T19:06:05.225506",
        "updated_at": "2022-04-04T14:41:28.643585",
        "name": "druid",
        "URI": "druid://host.docker.internal:8082/druid/v2/sql/"
      },
      {
        "id": 8,
        "description": "A Google Sheets connector",
        "read_only": true,
        "async_": false,
        "cost": 100,
        "created_at": "2022-01-17T19:06:05.309414",
        "updated_at": "2022-04-04T14:41:28.669827",
        "name": "gsheets",
        "URI": "gsheets://"
      },
      {
        "id": 9,
        "description": "A Postgres database",
        "read_only": false,
        "async_": false,
        "cost": 10,
        "created_at": "2022-01-17T19:06:05.334377",
        "updated_at": "2022-04-04T14:41:28.684467",
        "name": "postgres",
        "URI": "postgresql://username:FoolishPassword@host.docker.internal:5433/examples"
      }

You can run queries against any of these databases:

.. code-block:: bash

    $ curl -H "Content-Type: application/json" \
    > -d '{"database_id":9,"submitted_query":"SELECT 1 AS foo"}' \
    > http://127.0.0.1:8000/queries/ | jq
    {
      "database_id": 9,
      "catalog": null,
      "schema_": null,
      "id": "24bdf341-71a6-496d-9731-758be1ca685d",
      "submitted_query": "SELECT 1 AS foo",
      "executed_query": "SELECT 1 AS foo",
      "scheduled": "2022-04-04T17:35:23.116790",
      "started": "2022-04-04T17:35:23.116955",
      "finished": "2022-04-04T17:35:23.167996",
      "state": "FINISHED",
      "progress": 1,
      "results": [
        {
          "sql": "SELECT 1 AS foo",
          "columns": [
            {
              "name": "foo",
              "type": "NUMBER"
            }
          ],
          "rows": [
            [
              1
            ]
          ],
          "row_count": 1
        }
      ],
      "next": null,
      "previous": null,
      "errors": []
    }

To see the list of available metrics:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/ | jq
    [
      {
        "id": 6,
        "name": "core.num_comments",
        "description": "Number of comments",
        "created_at": "2022-01-17T19:06:09.215689",
        "updated_at": "2022-04-04T16:27:53.374001",
        "expression": "SELECT COUNT(*) FROM core.comments",
        "dimensions": [
          "core.comments/id",
          "core.comments/user_id",
          "core.comments/timestamp",
          "core.comments/text"
        ]
      }
    ]

To get data for a given metric:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/6/data/ | jq

You can also pass query parameters to group by a dimension (``d``) or filter (``f``):

.. code-block:: bash

    $ curl "http://localhost:8000/metrics/6/data/?d=core.comments/user_id&f=core.comments/user_id<4" | jq

Soon DJ will also have an API allowing metrics to be queried via SQL, eg:

.. code-block:: sql

    SELECT "core.num_comments"
    FROM metrics
    WHERE "core.comments".user_id < 4
    GROUP BY "core.comments".user_id


API docs
========

Once you have Docker running you can see the API docs at http://localhost:8000/docs.

Creating a PR
=============

When creating a PR, make sure to run ``make test`` to check for test coverage. You can also run ``make check`` to run the pre-commit hooks.

A few `fixtures <https://docs.pytest.org/en/7.1.x/explanation/fixtures.html#about-fixtures>`_ are `available <https://github.com/DataJunction/datajunction/blob/main/tests/conftest.py>`_ to help writing unit tests.
