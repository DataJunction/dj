============
Contributing
============

Pre-requisites
==============

DataJunction (DJ) is currently supported in Python 3.8, 3.9, and 3.10. It's recommended to use ``pyenv`` to create a virtual environment called "dj", so you can use the included ``Makefile``:

.. code-block:: bash

    $ pyenv virtualenv 3.8 dj  # or 3.9/3.10

Then you can just run ``make test`` to install all the dependencies.

DJ relies heavily on these libraries:

- `sqloxide <https://pypi.org/project/sqloxide/>`_, for SQL parsing.
- `SQLAlchemy <https://www.sqlalchemy.org/>`_ for running queries and fetching table metadata.
- `SQLModel <https://sqlmodel.tiangolo.com/>`_ for defining models.
- `FastAPI <https://fastapi.tiangolo.com/>`_ for APIs.

The SQLModel documentation is a good start, since it covers the basics of SQLAlchemy and FastAPI as well.

Running the examples
====================

The repository comes with a small set of examples in the ``examples/configs/`` directory, with some basic nodes built from 3 databases (Postgres, Druid, Google Sheets) and another example based on DBT tutorial (WIP).

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
        "name": "dbt.jaffle_shop.orders",
        "description": "Orders fact table",
        "created_at": "2022-09-30T03:51:26.269672+00:00",
        "updated_at": "2022-09-30T03:51:26.269685+00:00",
        "type": "source",
        "query": null,
        "columns": [
          {
            "name": "id",
            "type": "INT"
          },
          {
            "name": "user_id",
            "type": "INT"
          },
          {
            "name": "order_date",
            "type": "DATE"
          },
          {
            "name": "status",
            "type": "STR"
          },
          {
            "name": "_etl_loaded_at",
            "type": "DATETIME"
          }
        ]
      },
      {
        "id": 2,
        "name": "dbt.jaffle_shop.customers",
        "description": "Customer table",
        "created_at": "2022-09-30T03:51:26.363081+00:00",
        "updated_at": "2022-09-30T03:51:26.363096+00:00",
        "type": "source",
        "query": null,
        "columns": [
          {
            "name": "id",
            "type": "INT"
          },
          {
            "name": "first_name",
            "type": "STR"
          },
          {
            "name": "last_name",
            "type": "STR"
          }
        ]
      },
      ...
    ]

And metrics:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/ | jq
    [
      {
        "id": 8,
        "name": "basic.num_users",
        "description": "Number of users.",
        "created_at": "2022-09-30T03:51:29.193090+00:00",
        "updated_at": "2022-09-30T03:51:29.193124+00:00",
        "query": "SELECT SUM(num_users) FROM basic.transform.country_agg",
        "dimensions": [
          "basic.transform.country_agg.country",
          "basic.transform.country_agg.num_users"
        ]
      },
      {
        "id": 10,
        "name": "basic.num_comments",
        "description": "Number of comments",
        "created_at": "2022-09-30T03:51:30.376928+00:00",
        "updated_at": "2022-09-30T03:51:30.376937+00:00",
        "query": "SELECT COUNT(1) FROM basic.source.comments",
        "dimensions": [
          "basic.dimension.users.age",
          "basic.dimension.users.country",
          "basic.dimension.users.full_name",
          "basic.dimension.users.gender",
          "basic.dimension.users.id",
          "basic.dimension.users.preferred_language",
          "basic.dimension.users.secret_number",
          "basic.source.comments.id",
          "basic.source.comments.text",
          "basic.source.comments.timestamp",
          "basic.source.comments.user_id"
        ]
      }
    ]


To get data for a given metric:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/8/data/ | jq

You can also pass query parameters to group by a dimension (``d``) or filter (``f``):

.. code-block:: bash

    $ curl "http://localhost:8000/metrics/8/data/?d=basic.transform.country_agg.country" | jq
    $ curl "http://localhost:8000/metrics/8/data/?f=basic.transform.country_agg.country='France'" | jq

Similarly, you can request the SQL for a given metric with given constraints:

.. code-block:: bash

    $ curl "http://localhost:8000/metrics/8/sql/?d=basic.transform.country_agg.country" | jq
    {
      "database_id": 3,
      "sql": "SELECT sum("basic.transform.country_agg".num_users) AS sum_1, "basic.transform.country_agg".country \nFROM (SELECT "basic.source.users".country AS country, count("basic.source.users".id) AS num_users \nFROM (SELECT basic.dim_users.id AS id, basic.dim_users.full_name AS full_name, basic.dim_users.age AS age, basic.dim_users.country AS country, basic.dim_users.gender AS gender, basic.dim_users.preferred_language AS preferred_language \nFROM basic.dim_users) AS "basic.source.users" GROUP BY "basic.source.users".country) AS "basic.transform.country_agg" GROUP BY "basic.transform.country_agg".country"
    }

You can also run SQL queries against the metrics in DJ, using the special database with ID 0 and referencing a table called ``metrics``:

.. code-block:: sql

    SELECT "basic.num_comments"
    FROM metrics
    WHERE "basic.source.comments.user_id" < 4
    GROUP BY "basic.source.comments.user_id"


API docs
========

Once you have Docker running you can see the API docs at http://localhost:8000/docs.

Creating a PR
=============

When creating a PR, make sure to run ``make test`` to check for test coverage. You can also run ``make check`` to run the pre-commit hooks.

A few `fixtures <https://docs.pytest.org/en/7.1.x/explanation/fixtures.html#about-fixtures>`_ are `available <https://github.com/DataJunction/dj/blob/main/tests/conftest.py>`_ to help writing unit tests.

Adding new dependencies
=======================

When a PR introduces a new dependency, add them to ``setup.cfg`` under ``install_requires``. If the dependency version is less than ``1.0`` and you expect it to change often it's better to pin the dependency, eg:

.. code-block:: config

    some-package==0.0.1

Otherwise specify the package with a lower bound only:

.. code-block:: config

    some-package>=1.2.3

Don't use upper bounds in the dependencies. We have nightly unit tests that test if newer versions of dependencies will break.

Database migrations
===================

We use `Alembic <https://alembic.sqlalchemy.org/en/latest/index.html>`_ to manage schema migrations. If a PR introduces new models are changes existing ones a migration must be created.

1. Run the Docker container with ``docker compose up``.
2. Enter the ``dj`` container with ``docker exec -it dj bash``.
3. Run ``alembic revision --autogenerate -m "Description of the migration"``. This will create a file in the repository, under ``alembic/versions/``. Verify the file, checking that the upgrade and the downgrade functions make sense.
4. Still inside the container, run ``alembic upgrade head``. This will update the database schema to match the models.
5. Now run ``alembic downgrade $SHA``, where ``$SHA`` is the previous migration. You can see the hash with ``alembic history``.
6. Once you've confirmed that both the upgrade and downgrade work, upgrade again and commit the file.
