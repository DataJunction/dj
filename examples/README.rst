Running
=======

Run ``docker-compose`` at the project root:

.. code-block:: bash

    $ cd datajunction/
    $ docker compose up

This will run the DJ server with Redis for async queries, as well as a Druid cluster and a Postgres database. On startup the repository from ``examples/configs/`` will be compiled.

You can then see the available databases with:

.. code-block:: bash

    $ curl http://localhost:8000/databases/ | jq

You should see:

.. code-block:: json

    [
      {
        "id": 1,
        "created_at": "2022-01-02T23:07:04.228888",
        "updated_at": "2022-01-02T23:07:04.228895",
        "description": "An Apache Druid database",
        "read_only": true,
        "async_": false,
        "name": "druid",
        "URI": "druid://host.docker.internal:8082/druid/v2/sql/"
      },
      {
        "id": 2,
        "created_at": "2022-01-02T23:07:04.270360",
        "updated_at": "2022-01-02T23:07:04.270371",
        "description": "A Google Sheets connector",
        "read_only": true,
        "async_": false,
        "name": "gsheets",
        "URI": "gsheets://"
      },
      {
        "id": 3,
        "created_at": "2022-01-02T23:07:04.281625",
        "updated_at": "2022-01-02T23:07:04.281634",
        "description": "A Postgres database",
        "read_only": false,
        "async_": false,
        "name": "postgres",
        "URI": "postgresql://username:FoolishPassword@host.docker.internal:5433/examples"
      }
    ]

To run a query:

.. code-block:: bash

    $ curl -H "Content-Type: application/json" \
    > -d '{"database_id":3,"submitted_query":"SELECT 1 AS foo"}' \
    > http://127.0.0.1:8000/queries/ | jq

And you should see:

.. code-block:: json

    {
      "database_id": 3,
      "catalog": null,
      "schema_": null,
      "id": "a1cbb5d8-d0e9-4063-9848-4d91c60b720f",
      "submitted_query": "SELECT 1 AS foo",
      "executed_query": "SELECT 1 AS foo",
      "scheduled": "2022-01-02T23:14:20.065298",
      "started": "2022-01-02T23:14:20.065369",
      "finished": "2022-01-02T23:14:20.106211",
      "state": "FINISHED",
      "progress": 1,
      "results": {
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
        ]
      },
      "errors": []
    }

Troubleshooting
===============

If the Druid data doesn't load, you need to fix the permissions:

.. code-block:: bash

    $ docker exec -u root -it coordinator sh
    $ chmod 777 /opt/shared
    $ docker-compose restart druid_ingest
