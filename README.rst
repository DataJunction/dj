------------
DataJunction
------------

DataJunction (DJ) is a **metrics platform** to store and manage metric definitions. Metrics are defined using **ANSI SQL**, are **database agnostic** and can be computed via a **REST API** or **SQL** interface.

What does that mean?
--------------------

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

    $ uvicorn dj.api.main:app --host 127.0.0.1 --port 8000 --reload

Now, if we want to compute the metric in our Hive warehouse we can build a pipeline that requests the Hive SQL:

.. code-block:: bash

    curl "http://localhost:8000/metrics/basic.num_comments/sql/?database_name=postgres" | jq

.. code-block:: bash

    {
      "database_id": 3,
      "sql": "SELECT count(1) AS cnt \nFROM (SELECT basic.comments.id AS id, basic.comments.user_id AS user_id, basic.comments.timestamp AS timestamp, basic.comments.text AS text \nFROM basic.comments) AS \"basic.source.comments\""
    }

We can also filter and group our metric by any of its dimensions:

.. code-block:: bash

    curl "http://localhost:8000/metrics/basic.num_comments/" | jq

.. code-block:: bash

    {
      "id": 12,
      "name": "basic.num_comments",
      "description": "Number of comments",
      "created_at": "2023-01-31T04:32:01.091728",
      "updated_at": "2023-01-31T04:32:01.091755",
      "query": "SELECT COUNT(1) AS cnt\nFROM basic.source.comments",
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

For example, if we want to group the metric by the user ID, to see how many comments each user made, while filtering out non-positive user IDs:

.. code-block:: bash

    curl "http://localhost:8000/metrics/basic.num_comments/sql/?database_name=postgres&d=basic.source.comments.user_id&f=basic.source.comments.user_id>0" | jq

If instead we want the actual data, instead of the SQL:

.. code-block:: bash

    curl "http://localhost:8000/metrics/basic.num_comments/data/?database_name=postgres&d=basic.source.comments.user_id&f=basic.source.comments.user_id>0" | jq

And if we omit the ``database_name`` DJ will compute the data using the fastest database (ie, the one with lowest ``cost``). It's also possible to specify tables with different costs:

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
---------------

While all the functionality above currently works, DJ is still not ready for production use. Only a very small number of functions are supported, and we are still working towards a 0.1 release. If you are interested in helping take a look at the `issues marked with the "good first issue" label <https://github.com/DataJunction/dj/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`_.
