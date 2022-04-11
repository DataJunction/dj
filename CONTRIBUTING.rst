============
Contributing
============

Pre-requisites
==============

DataJunction (DJ) is currently supported in Python 3.8, 3.9, and 3.10. It's recommended to use ``pyenv`` to create a virtual environment called "datajunction", so you can use the included ``Makefile``:

.. code-block:: bash

    $ pyenv virtualenv 3.8 datajunction  # or 3.9/3.10

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
        "id": 0,
        "description": "The DJ meta database",
        "read_only": true,
        "async_": false,
        "cost": 1,
        "created_at": "2022-04-10T20:22:58.274082",
        "updated_at": "2022-04-10T20:22:58.274092",
        "name": "dj",
        "URI": "dj://localhost:8000/0"
      },
      {
        "id": 1,
        "description": "An in memory SQLite database for tableless queries",
        "read_only": true,
        "async_": false,
        "cost": 0,
        "created_at": "2022-04-10T20:22:58.275177",
        "updated_at": "2022-04-10T20:22:58.275183",
        "name": "in-memory",
        "URI": "sqlite://"
      },
      {
        "id": 2,
        "description": "An Apache Druid database",
        "read_only": true,
        "async_": false,
        "cost": 1,
        "created_at": "2022-04-10T20:22:58.285035",
        "updated_at": "2022-04-10T20:22:58.291059",
        "name": "druid",
        "URI": "druid://host.docker.internal:8082/druid/v2/sql/"
      },
      {
        "id": 3,
        "description": "A Postgres database",
        "read_only": false,
        "async_": false,
        "cost": 10,
        "created_at": "2022-04-10T20:22:58.298188",
        "updated_at": "2022-04-10T20:22:58.305128",
        "name": "postgres",
        "URI": "postgresql://username:FoolishPassword@host.docker.internal:5433/examples"
      },
      {
        "id": 4,
        "description": "A Google Sheets connector",
        "read_only": true,
        "async_": false,
        "cost": 100,
        "created_at": "2022-04-10T20:22:58.310020",
        "updated_at": "2022-04-10T20:22:58.317108",
        "name": "gsheets",
        "URI": "gsheets://"
      }
    ]

You can run queries against any of these databases:

.. code-block:: bash

    $ curl -H "Content-Type: application/json" \
    > -d '{"database_id":1,"submitted_query":"SELECT 1 AS foo"}' \
    > http://127.0.0.1:8000/queries/ | jq
    {
      "database_id": 1,
      "catalog": null,
      "schema_": null,
      "id": "5cc9cc71-02c2-4c73-a0d9-f9c752f0762b",
      "submitted_query": "SELECT 1 AS foo",
      "executed_query": "SELECT 1 AS foo",
      "scheduled": "2022-04-11T01:02:56.221241",
      "started": "2022-04-11T01:02:56.221289",
      "finished": "2022-04-11T01:02:56.222603",
      "state": "FINISHED",
      "progress": 1,
      "results": [
        {
          "sql": "SELECT 1 AS foo",
          "columns": [
            {
              "name": "foo",
              "type": "STR"
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

To see the list of available nodes:

.. code-block:: bash

    $ curl http://localhost:8000/nodes/ | jq
    [
      {
        "id": 1,
        "name": "core.comments",
        "description": "A fact table with comments",
        "created_at": "2022-04-10T20:22:58.345198",
        "updated_at": "2022-04-10T20:22:58.345201",
        "type": "source",
        "expression": null,
        "columns": [
          {
            "id": 14,
            "dimension_id": null,
            "dimension_column": null,
            "name": "id",
            "type": "INT"
          },
          {
            "id": 15,
            "dimension_id": null,
            "dimension_column": null,
            "name": "user_id",
            "type": "INT"
          },
          {
            "id": 16,
            "dimension_id": null,
            "dimension_column": null,
            "name": "timestamp",
            "type": "DATETIME"
          },
          {
            "id": 17,
            "dimension_id": null,
            "dimension_column": null,
            "name": "text",
            "type": "STR"
          },
          {
            "id": 18,
            "dimension_id": null,
            "dimension_column": null,
            "name": "__time",
            "type": "DATETIME"
          },
          {
            "id": 19,
            "dimension_id": null,
            "dimension_column": null,
            "name": "count",
            "type": "INT"
          }
        ]
      },
      {
        "id": 2,
        "name": "core.users",
        "description": "A user dimension table",
        "created_at": "2022-04-10T20:23:01.333020",
        "updated_at": "2022-04-10T20:23:01.333024",
        "type": "dimension",
        "expression": null,
        "columns": [
          {
            "id": 33,
            "dimension_id": null,
            "dimension_column": null,
            "name": "id",
            "type": "INT"
          },
          {
            "id": 34,
            "dimension_id": null,
            "dimension_column": null,
            "name": "full_name",
            "type": "STR"
          },
          {
            "id": 35,
            "dimension_id": null,
            "dimension_column": null,
            "name": "age",
            "type": "INT"
          },
          {
            "id": 36,
            "dimension_id": null,
            "dimension_column": null,
            "name": "country",
            "type": "STR"
          },
          {
            "id": 37,
            "dimension_id": null,
            "dimension_column": null,
            "name": "gender",
            "type": "STR"
          },
          {
            "id": 38,
            "dimension_id": null,
            "dimension_column": null,
            "name": "preferred_language",
            "type": "STR"
          },
          {
            "id": 39,
            "dimension_id": null,
            "dimension_column": null,
            "name": "secret_number",
            "type": "FLOAT"
          }
        ]
      },
      {
        "id": 3,
        "name": "core.num_comments",
        "description": "Number of comments",
        "created_at": "2022-04-10T20:23:01.961078",
        "updated_at": "2022-04-10T20:23:01.961083",
        "type": "metric",
        "expression": "SELECT COUNT(*) FROM core.comments",
        "columns": [
          {
            "id": 40,
            "dimension_id": null,
            "dimension_column": null,
            "name": "_col0",
            "type": "INT"
          }
        ]
      }
    ]

And metrics:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/ | jq
    [
      {
        "id": 3,
        "name": "core.num_comments",
        "description": "Number of comments",
        "created_at": "2022-04-10T20:23:01.961078",
        "updated_at": "2022-04-10T20:23:01.961083",
        "expression": "SELECT COUNT(*) FROM core.comments",
        "dimensions": [
          "core.comments.id",
          "core.comments.user_id",
          "core.comments.timestamp",
          "core.comments.text",
          "core.comments.__time",
          "core.comments.count"
        ]
      }
    ]
        

To get data for a given metric:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/3/data/ | jq

You can also pass query parameters to group by a dimension (``d``) or filter (``f``):

.. code-block:: bash

    $ curl "http://localhost:8000/metrics/3/data/?d=core.comments/user_id&f=core.comments/user_id<4" | jq

Similarly, you can request the SQL for a given metric with given constraints:

.. code-block:: bash

    $ curl "http://localhost:8000/metrics/3/sql/?d=core.comments.user_id" | jq
    {
      "database_id": 7,
      "sql": "SELECT count('*') AS \"count_1\" \nFROM (SELECT \"druid\".\"comments\".\"__time\" AS \"__time\", \"druid\".\"comments\".\"count\" AS \"count\", \"druid\".\"comments\".\"id\" AS \"id\", \"druid\".\"comments\".\"text\" AS \"text\", \"druid\".\"comments\".\"user_id\" AS \"user_id\" \nFROM \"druid\".\"comments\") AS \"core.comments\" GROUP BY \"core.comments\".\"user_id\""
    }

You can also run SQL queries against the metrics in DJ, using the special database with ID 0 and referencing a table called ``metrics``:

.. code-block:: sql

    SELECT "core.num_comments"
    FROM metrics
    WHERE "core.comments.user_id" < 4
    GROUP BY "core.comments.user_id"


API docs
========

Once you have Docker running you can see the API docs at http://localhost:8000/docs.

Creating a PR
=============

When creating a PR, make sure to run ``make test`` to check for test coverage. You can also run ``make check`` to run the pre-commit hooks.

A few `fixtures <https://docs.pytest.org/en/7.1.x/explanation/fixtures.html#about-fixtures>`_ are `available <https://github.com/DataJunction/datajunction/blob/main/tests/conftest.py>`_ to help writing unit tests.
