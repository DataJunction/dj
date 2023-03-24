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

The repository should be run in conjunction with [`djqs`](https://github.com/DataJunction/djqs), which will load a
`postgres-roads` database with example data. In order to get this up and running:
* ``docker compose up`` from DJ to get the DJ metrics service up (defaults to run on port 8000)
* ``docker compose up`` in DJQS to get the DJ query service up (defaults to run on port 8001)

Once both are up, you can fire up `juypter notebook` to run the `Modeling the Roads Example Database.ipynb`,
which will create all of the relevant nodes and provide some examples of API interactions.

You can check that everything is working by querying the list of available catalogs (install `jq <https://stedolan.github.io/jq/>`_ if you don't have it):

.. code-block:: bash

    % curl http://localhost:8000/catalogs/ | jq
    [
      {
        "name": "default",
        "engines": [
          {
            "name": "postgres",
            "version": "",
            "uri": "postgresql://dj:dj@postgres-roads:5432/djdb"
          }
        ]
      }
    ]

To see the list of available nodes:

.. code-block:: bash

    $ curl http://localhost:8000/nodes/ | jq
    [
      {
        "node_revision_id": 1,
        "node_id": 1,
        "type": "source",
        "name": "repair_orders",
        "display_name": "Repair Orders",
        "version": "v1.0",
        "status": "valid",
        "mode": "published",
        "catalog": {
          "id": 1,
          "uuid": "c2363d4d-ce0c-4eb2-9b1e-28743970f859",
          "created_at": "2023-03-17T15:45:15.012784+00:00",
          "updated_at": "2023-03-17T15:45:15.012795+00:00",
          "extra_params": {},
          "name": "default"
        },
        "schema_": "roads",
        "table": "repair_orders",
        "description": "Repair orders",
        "query": null,
        "availability": null,
        "columns": [
          {
            "name": "repair_order_id",
            "type": "INT",
            "attributes": []
          },
          {
            "name": "municipality_id",
            "type": "STR",
            "attributes": []
          },
          {
            "name": "hard_hat_id",
            "type": "INT",
            "attributes": []
          },
          {
            "name": "order_date",
            "type": "TIMESTAMP",
            "attributes": []
          },
          {
            "name": "required_date",
            "type": "TIMESTAMP",
            "attributes": []
          },
          {
            "name": "dispatched_date",
            "type": "TIMESTAMP",
            "attributes": []
          },
          {
            "name": "dispatcher_id",
            "type": "INT",
            "attributes": []
          }
        ],
        "updated_at": "2023-03-17T15:45:18.456072+00:00",
        "materialization_configs": [],
        "created_at": "2023-03-17T15:45:18.448321+00:00",
        "tags": []
      },
      ...
    ]

And metrics:

.. code-block:: bash

    $ curl http://localhost:8000/metrics/ | jq
    [
      {
        "id": 21,
        "name": "num_repair_orders",
        "display_name": "Num Repair Orders",
        "current_version": "v1.0",
        "description": "Number of repair orders",
        "created_at": "2023-03-17T15:45:27.589799+00:00",
        "updated_at": "2023-03-17T15:45:27.590304+00:00",
        "query": "SELECT count(repair_order_id) as num_repair_orders FROM repair_orders",
        "dimensions": [
          "dispatcher.company_name",
          "dispatcher.dispatcher_id",
          "dispatcher.phone",
          "hard_hat.address",
          "hard_hat.birth_date",
          "hard_hat.city",
          "hard_hat.contractor_id",
          "hard_hat.country",
          "hard_hat.first_name",
          "hard_hat.hard_hat_id",
          "hard_hat.hire_date",
          "hard_hat.last_name",
          "hard_hat.manager",
          "hard_hat.postal_code",
          "hard_hat.state",
          "hard_hat.title",
          "municipality_dim.contact_name",
          "municipality_dim.contact_title",
          "municipality_dim.local_region",
          "municipality_dim.municipality_id",
          "municipality_dim.municipality_type_desc",
          "municipality_dim.municipality_type_id",
          "municipality_dim.phone",
          "municipality_dim.state_id",
          "repair_orders.dispatched_date",
          "repair_orders.dispatcher_id",
          "repair_orders.hard_hat_id",
          "repair_orders.municipality_id",
          "repair_orders.order_date",
          "repair_orders.repair_order_id",
          "repair_orders.required_date"
        ]
      },
      {
        "id": 22,
        "name": "avg_repair_price",
        "display_name": "Avg Repair Price",
        "current_version": "v1.0",
        "description": "Average repair price",
        "created_at": "2023-03-17T15:45:28.121435+00:00",
        "updated_at": "2023-03-17T15:45:28.121836+00:00",
        "query": "SELECT avg(price) as avg_repair_price FROM repair_order_details",
        "dimensions": [
          "repair_order.dispatcher_id",
          "repair_order.hard_hat_id",
          "repair_order.municipality_id",
          "repair_order.repair_order_id",
          "repair_order_details.discount",
          "repair_order_details.price",
          "repair_order_details.quantity",
          "repair_order_details.repair_order_id",
          "repair_order_details.repair_type_id"
        ]
      },
      ...
    ]


To get data for a given metric:

.. code-block:: bash

    $ curl http://localhost:8000/data/avg_repair_price/ | jq

You can also pass query parameters to group by a dimension or filter:

.. code-block:: bash

    $ curl "http://localhost:8000/data/avg_time_to_dispatch/?dimensions=dispatcher.company_name" | jq
    $ curl "http://localhost:8000/data/avg_time_to_dispatch/?filters=hard_hat.state='AZ'" | jq

Similarly, you can request the SQL for a given metric with given constraints:

.. code-block:: bash

    $ curl "http://localhost:8000/sql/avg_time_to_dispatch/?dimensions=dispatcher.company_name" | jq
    {
      "sql": "SELECT  avg(repair_orders.dispatched_date - repair_orders.order_date) AS avg_time_to_dispatch,\n\tdispatcher.company_name \n FROM \"roads\".\"repair_orders\" AS repair_orders\nLEFT JOIN (SELECT  dispatchers.company_name,\n\tdispatchers.dispatcher_id,\n\tdispatchers.phone \n FROM \"roads\".\"dispatchers\" AS dispatchers\n \n) AS dispatcher\n        ON repair_orders.dispatcher_id = dispatcher.dispatcher_id \n GROUP BY  dispatcher.company_name"
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

We use `Alembic <https://alembic.sqlalchemy.org/en/latest/index.html>`_ to manage schema migrations. If a PR introduces new models or changes existing ones a migration must be created.

1. Run the Docker container with ``docker compose up``.
2. Enter the ``dj`` container with ``docker exec -it dj bash``.
3. Run ``alembic revision --autogenerate -m "Description of the migration"``. This will create a file in the repository, under ``alembic/versions/``. Verify the file, checking that the upgrade and the downgrade functions make sense.
4. Still inside the container, run ``alembic upgrade head``. This will update the database schema to match the models.
5. Now run ``alembic downgrade $SHA``, where ``$SHA`` is the previous migration. You can see the hash with ``alembic history``.
6. Once you've confirmed that both the upgrade and downgrade work, upgrade again and commit the file.

If the migrations include ``alter_column`` or ``drop_column`` make sure to wrap them in a ``batch_alter_table`` context manager so that they work correctly with SQLite. You can see `an example here <https://github.com/DataJunction/dj/pull/224/files#diff-22327a751511fb5eba403e0f30e124c08543243f67c2d09cee4cd756a2ef9df9R27-R28>`_.

Development tips
===================

Using ``PYTEST_ARGS`` with ``make test``
----------------------------------------

If you'd like to pass additional arguments to pytest when running `make test`, you can define them as ``PYTEST_ARGS``. For example, you can include
`--fixtures` to see a list of all fixtures.

.. code-block:: sh

    make test PYTEST_ARGS="--fixtures"

Running a Subset of Tests
-------------------------

When working on tests, it's common to want to run a specific test by name. This can be done by passing ``-k`` as an additional pytest argument along
with a string expression. Pytest will only run tests which contain names that match the given string expression.

.. code-block:: sh

    make test PYTEST_ARGS="-k test_main_compile"

Running TPC-DS Parsing Tests
-------------------------

A TPC-DS test suite is included but skipped by default. As we incrementally build support for various SQL syntax into the DJ
SQL AST, it's helpful to run these tests using the `--tpcds` flag.

.. code-block:: sh

    make test PYTEST_ARGS="--tpcds"

You can run only the TPC-DS tests without the other tests using a `-k` filter.

.. code-block:: sh

    make test PYTEST_ARGS="--tpcds -k tpcds"

Another useful option is matching on the full test identifier to run the test for a single specific query file from the
parametrize list. This is useful when paired with `--pdb` to drop into the debugger.

.. code-block:: sh

    make test PYTEST_ARGS="--tpcds --pdb -k test_parsing_ansi_tpcds_queries[./ansi/query1.sql]"

If you prefer to use tox, these flags all work the same way.

.. code-block:: sh

    tox tests/sql/parsing/queries/tpcds/test_tpcds.py::test_parsing_sparksql_tpcds_queries -- --tpcds

Enabling ``pdb`` When Running Tests
-----------------------------------

If you'd like to drop into ``pdb`` when a test fails, or on a line where you've added ``pdb.set_trace()``, you can pass ``--pdb`` as a pytest argument.

.. code-block:: sh

    make test PYTEST_ARGS="--pdb"

Using ``pdb`` In Docker
-----------------------

The included docker compose files make it easy to get a development environment up and running locally. When debugging or working on a new feature,
it's helpful to set breakpoints in the source code to drop into ``pdb`` at runtime. In order to do this while using the docker compose setup, there
are three steps.

1. Set a trace in the source code on the line where you'd like to drop into ``pdb``.

.. code-block:: python

  import pdb; pdb.set_trace()

2. In the docker compose file, enable ``stdin_open`` and ``tty`` on the service you'd like debug.

.. code-block:: YAML

  services:
    dj:
      stdin_open: true
      tty: true
      ...

3. Once the docker environment is running, attach to the container.

.. code-block:: sh

  docker attach dj

When the breakpoint is hit, the attached session will enter an interactive ``pdb`` session.

ANTLR
-----

Generating the ANTLR Parser
---------------------------

Install the ANTLR generator tool.

.. code-block:: sh

  pip install antlr4-tools

While in the `dj/sql/parsing/backends/antlr4/grammar/` directory, generate the parser by running the following CLI command.

.. code-block:: sh

  antlr4 -Dlanguage=Python3 -visitor SqlBaseLexer.g4 SqlBaseParser.g4 -o generated

A python 3 ANTLR parser will be generated in `dj/sql/parsing/backends/antlr4/grammar/generated/`.

Creating a Diagram from the Grammar
-----------------------------------

Use https://bottlecaps.de/convert/ to go from ANTLR4 -> EBNF

Input the EBNF into https://bottlecaps.de/rr/ui
