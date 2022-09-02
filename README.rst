============
DataJunction
============


    A metrics repository


DataJunction (DJ) is a repository of **metric definitions**. Metrics are defined using **ANSI SQL** and are **database agnostic**. Metrics can then be computed via a **REST API** or **SQL**.

What does that mean?
====================

DJ allows users to define metrics once, and reuse them in different databases. This offers a couple benefits:

1. The same metric definition can be used in different databases, ensuring that the results are consistent. A daily pipeline might use Hive to compute metrics in batch mode, while a dashboard application might use a faster database like Druid.
2. Users don't have to worry where or how a metric is computed -- all they need to know is the metric name. Tools can leverage the DJ semantic layer to allow users to easily filter and group metrics by any available dimensions.

As an example, imagine that we have 2 databases, Hive and Trino. In DJ they are represented as YAML files:

.. code-block:: YAML

    # databases/hive.yaml
    description: A (slow) Hive database
    URI: hive://localhost:10000/default
    read_only: false
    cost: 10

.. code-block:: YAML

    # databases/trino.yaml
    description: A (fast) Trino database
    URI: trino://localhost:8080/hive/default
    read_only: false
    cost: 1

Now imagine we have a table in those databases, also represented as a YAML file:

.. code-block:: YAML

    # nodes/comments.yaml
    type: source
    description: A fact table with comments
    tables:
      hive:
        - catalog: null
          schema: default
          table: fact_comments
      trino:
        - catalog: hive
          schema: default
          table: fact_comments

So far we've only described what already exists. Let's add a metric called ``num_comments``, which keeps track of how many comments have been posted so far:

.. code-block:: YAML

    # nodes/num_comments.yaml
    type: metric
    description: The number of comments
    query: SELECT COUNT(*) FROM comments

Now we run a command to parse all the configuration files and index them in a database:

.. code-block:: bash

    $ dj compile

And we start a development server:

.. code-block:: bash

    $ uvicorn datajunction.api.main:app --host 127.0.0.1 --port 8000 --reload

Now, if we want to compute the metric in our Hive warehouse we can build a pipeline that requests the Hive SQL:

.. code-block:: bash

    % curl "http://localhost:8000/metrics/2/sql/?database_id=1"
    {
      "database_id": 1,
      "sql": "SELECT count('*') AS count_1 \nFROM (SELECT default.fact_comments.id AS id, default.fact_comments.user_id AS user_id, default.fact_comments.timestamp AS timestamp, default.fact_comments.text AS text \nFROM default.fact_comments) AS \"comments\""
    }

We can also filter and group our metric by any of its dimensions:

.. code-block:: bash

    % curl http://localhost:8000/metrics/2/
    {
      "id": 2,
      "name": "num_comments",
      "description": "A fact table with comments",
      "created_at": "2022-01-17T19:06:09.215689",
      "updated_at": "2022-04-04T16:27:53.374001",
      "query": "SELECT COUNT(*) FROM comments",
      "dimensions": [
        "comments.id",
        "comments.user_id",
        "comments.timestamp",
        "comments.text"
      ]
    }

For example, if we want to group the metric by the user ID, to see how many comments each user made, while filtering out non-positive user IDs:

.. code-block:: bash

    % curl "http://localhost:8000/metrics/2/sql/?database_id=1&d=comments.user_id&f=comments.user_id>0"

If instead we want the actual data, instead of the SQL:

.. code-block:: bash

    % curl "http://localhost:8000/metrics/2/data/?database_id=1&d=comments.user_id&f=comments.user_id>0"

And if we omit the ``database_id`` DJ will compute the data using the fastest database (ie, the one with lowest ``cost``). It's also possible to specify tables with different costs:

.. code-block:: YAML

    # nodes/users.yaml
    description: A dimension table with user information
    type: dimension
    tables:
      hive:
        - catalog: null
          schema: default
          table: dim_users
          cost: 10
        - catalog: null
          schema: default
          table: dim_fast_users
          cost: 1

The tables ``dim_users`` and ``dim_fast_users`` can have different columns. For example, ``dim_fast_users`` could have only a subset of the columns in ``dim_users``, the ones that can be quickly populated. DJ will use the fast table if the available columns can satisfy a given query, otherwise it will fallback to the slow table.

Getting started
===============

While all the functionality above currently works, DJ is still not ready for production use. Only a very small number of functions are supported, and we are still working towards a 0.1 release. If you are interested in helping take a look at the `issues marked with the "good first issue" label <https://github.com/DataJunction/datajunction/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`_.
