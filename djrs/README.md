# DJ Reflection Service

The reflection service polls the DJ core service for all nodes with associated tables, whether source
tables or materialized tables. For each node, it refreshes the node's schema based on the associated
table's schema that it retrieves from the query service. It also retrieves the available partitions and
the valid through timestamp of these tables and reflects them accordingly to DJ core.

This service uses a celery beat scheduler, with a configurable polling interval that defaults to once per
hour and async tasks for each node's reflection.
