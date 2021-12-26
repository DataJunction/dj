Running
=======

.. code-block:: bash

    $ cd docker/
    $ docker compose up

If the Druid data doesn't load, you need to fix the permissions:

.. code-block:: bash

    $ docker exec -u root -it coordinator sh
    $ chmod 777 /opt/shared
    $ docker-compose restart druid_ingest

Install the required SQLAlchemy dialects:

.. code-block:: bash

    $ pip install 'shillelagh[gsheetsapi]' pydruid psycopg2-binary

And run:
