Running
=======

Run ``docker-compose`` at the project root:

.. code-block:: bash

    $ cd dj/
    $ docker compose up

This will run the DJ server with Redis for async queries, as well as a Druid cluster and a Postgres database. On startup the repository from ``examples/configs/`` will be compiled.

You can then see the available databases with:

.. code-block:: bash

    $ curl http://localhost:8001/databases/ | jq

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
    > -d '{"database_id":2,"submitted_query":"SELECT 1 AS foo"}' \
    > http://127.0.0.1:8001/queries/ | jq

And you should see:

.. code-block:: json

    {
      "database_id": 3,
      "catalog": null,
      "schema_": null,
      "id": "db6c5ef8-bb8c-4972-ad08-9052eaa0c288",
      "submitted_query": "SELECT 1 AS foo",
      "executed_query": "SELECT 1 AS foo",
      "scheduled": "2022-01-03T01:09:15.164400",
      "started": "2022-01-03T01:09:15.164467",
      "finished": "2022-01-03T01:09:15.217595",
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
          ]
        }
      ],
      "errors": []
    }

Alternative Docker Compose Setups
=================================

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
===============

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
