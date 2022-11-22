============
Architecture
============

The source of truth in DataJunction (DJ) is a repository of YAML files (see the `examples <https://github.com/DataJunction/djqs/tree/main/examples/configs>`_). These files describe database connections and nodes. The nodes form a DAG (Directed Acyclic Graph), and relationships are inferred from their SQL queries:

.. code-block:: YAML

    # parent.yaml
    description: A parent node
    tables:
      postgres:
        catalog: null
        schema: public
        table: parent_table

.. code-block:: YAML

    # child.yaml
    description: A child node
    query: SELECT * FROM parent

In the example above we have 2 nodes, ``parent`` and ``child``, forming the DAG ``parent -> child``.

The command ``dj compile`` will parse all the YAML files, building a graph of nodes with full schema. The schema of root nodes (like ``parent``) are fetched from the database using SQLAlchemy, while the schema of downstream nodes (like ``child``) are inferred by analysing their queries. This information is then stored in a database.

This information and more can be exposed by an API. To run a DJ server:

.. code-block:: bash

    $ alembic upgrade head
    $ pip install uvicorn
    $ uvicorn djqs.api.main:app --host 0.0.0.0 --port 8001 --reload

Both the CLI (``dj compile``) and the web service read their configuration from a ``.env`` file:

.. code-block:: YAML

   NAME="Example DJ server"
   DESCRIPTION="A simple DJ server running in Docker"

   # directory where the config files are
   REPOSITORY=examples/configs

   # SQLAlchemy URI where the nodes should be indexed to
   INDEX=sqlite:///examples/configs/djqs.db

   # a broker used to store results from queries
   CELERY_BROKER=redis://host.docker.internal:6379/1

   # a cache used to fetch paginated results
   REDIS_CACHE=redis://host.docker.internal:6379/0

Transpilation
=============

In DJ metrics are defined as nodes with a single aggregation in the ``SELECT`` statement (like ``child`` above). When a metric needs to be computed DJ analyses the whole DAG, stopping on nodes that are materialized. In the example above, because ``child`` is not materialized, the resulting query would look like this:

.. code-block:: SQL

    SELECT * FROM (SELECT col1, col2 FROM parent_table) AS parent_table;

Note that the reference to ``parent`` in the metric definition (``SELECT * FROM parent``) was replaced by the parent definition.

The resulting query then needs to be transpiled to database-specific SQL in order to be executed. To transpile the query, DJ first parses it to a tree using ``sqloxide``. This tree is then converted into a SQLAlchemy `query <https://docs.sqlalchemy.org/en/14/core/expression_api.html>`_, which is an intermediary format that can be executed in multiple databases.
