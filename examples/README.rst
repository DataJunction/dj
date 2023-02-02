Running
-------

Run ``docker-compose`` at the project root:

.. code-block:: bash

    $ cd dj/
    $ docker compose up

This will run the DJ server with Redis for async queries, as well as a Druid cluster and a Postgres database. On startup the repository from ``examples/configs/`` will be compiled.

You can then list the available databases.

.. code-block:: bash

    curl http://localhost:8000/databases/ | jq

.. code-block:: json

    [
      {
        "id": -1,
        "uuid": "3619eeba-d628-4ab1-9dd5-65738ab3c02f",
        "description": "An in memory SQLite database for tableless queries",
        "extra_params": {},
        "read_only": true,
        "async_": false,
        "cost": 0,
        "created_at": "2023-01-31T04:31:53.699835",
        "updated_at": "2023-01-31T04:31:53.699863",
        "name": "in-memory",
        "URI": "sqlite://"
      },
      {
        "id": 0,
        "uuid": "594804bf-47cb-426c-83c4-94a348e95972",
        "description": "The DJ meta database",
        "extra_params": {},
        "read_only": true,
        "async_": false,
        "cost": 1,
        "created_at": "2023-01-31T04:31:53.690792",
        "updated_at": "2023-01-31T04:31:53.690858",
        "name": "dj",
        "URI": "dj://localhost:8000/0"
      },
      {
        "id": 1,
        "uuid": "48765763-4c1d-48a1-bf6c-fab9e4fad9b6",
        "description": "An Apache Druid database",
        "extra_params": {},
        "read_only": true,
        "async_": false,
        "cost": 1,
        "created_at": "2023-01-31T04:31:53.793496",
        "updated_at": "2023-01-31T04:31:53.795213",
        "name": "druid",
        "URI": "druid://druid_broker:8082/druid/v2/sql/"
      },
      {
        "id": 2,
        "uuid": "136d16ba-b953-4a3b-9a3d-037a059c91db",
        "description": "A Google Sheets connector",
        "extra_params": {
          "catalog": {
            "comments": "https://docs.google.com/spreadsheets/d/1SkEZOipqjXQnxHLMr2kZ7Tbn7OiHSgO99gOCS5jTQJs/edit#gid=1811447072",
            "users": "https://docs.google.com/spreadsheets/d/1SkEZOipqjXQnxHLMr2kZ7Tbn7OiHSgO99gOCS5jTQJs/edit#gid=0"
          }
        },
        "read_only": true,
        "async_": false,
        "cost": 100,
        "created_at": "2023-01-31T04:31:53.906079",
        "updated_at": "2023-01-31T04:31:53.909173",
        "name": "gsheets",
        "URI": "gsheets://"
      },
      {
        "id": 3,
        "uuid": "b87e4ded-e6fc-496d-a171-70b396b129ff",
        "description": "A Postgres database",
        "extra_params": {
          "connect_args": {
            "sslmode": "prefer"
          }
        },
        "read_only": false,
        "async_": false,
        "cost": 10,
        "created_at": "2023-01-31T04:31:53.967998",
        "updated_at": "2023-01-31T04:31:53.969566",
        "name": "postgres",
        "URI": "postgresql://username:FoolishPassword@postgres_examples:5432/examples"
      }
    ]

You can also run queries.

.. code-block:: bash

    curl -H "Content-Type: application/json" \
    -d '{"database_id": 2, "submitted_query": "SELECT 1 AS foo"}' \
    http://localhost:8000/queries/ | jq

.. code-block:: json

    {
      "database_id": 2,
      "catalog": null,
      "schema": null,
      "id": "3a6f013e-a2f2-47f1-9fd8-c3b6d7f2094e",
      "submitted_query": "SELECT 1 AS foo",
      "executed_query": "SELECT 1 AS foo",
      "scheduled": "2023-01-31T05:21:19.206699",
      "started": "2023-01-31T05:21:19.206929",
      "finished": "2023-01-31T05:21:21.576516",
      "state": "FINISHED",
      "progress": 1,
      "results": [
        {
          "sql": "SELECT 1 AS foo",
          "columns": [
            {
              "name": "foo",
              "type": "BYTES"
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

Alternative Docker Compose Setups
---------------------------------

The default docker compose setup includes the minimally required services to run a DJ server and the metadata is persisted
in Postgres. However, DJ leverages SQL Alchemy to enable flexibility when it comes to reading source databases as well as storing
its own metadata. Variations of the default docker compose environments can be selected using one of the available override
docker compose files which add, remove, or modify services.

.. list-table:: Docker Compose Optional Overrides
   :widths: 15 10 30
   :header-rows: 1

   * - Name
     - Command
     - Description
   * - SQLite
     - docker compose up
     - DJ server backed by SQLite
   * - Postgres
     - docker compose -f docker-compose.yml -f docker-compose.postgres.yml up
     - DJ server backed by Postgres
   * - Postgres + Druid
     - docker compose -f docker-compose.yml -f docker-compose.postgres.yml -f docker-compose.druid.yml up
     - An extension of the Postgres setup that includes Druid
   * - CockroachDB
     - docker compose -f docker-compose.yml -f docker-compose.cockroachdb.yml up
     - DJ server backed by CockroachDB

Troubleshooting
---------------

1. If the Druid data doesn't load, you may need to fix these permissions:

  .. code-block:: bash

      $ docker exec -u root -it druid_coordinator sh
      $ chmod 777 /opt/shared
      $ exit
      $ docker-compose restart druid_ingest
      $ cd docker/
      $ curl -H 'Content-Type:application/json' -d @druid_spec.json http://localhost:8081/druid/indexer/v1/task

2. If you see the following errors from your druid service:

  .. code-block:: bash

      HTML Error: org.apache.druid.java.util.common.ISE: No default server found

Then you may need to increase your Docker memory size. This likely varies per machine but 12GB is probably needed.
