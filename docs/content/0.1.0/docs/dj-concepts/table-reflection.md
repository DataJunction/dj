---
weight: 90
---

# Table Reflection

Source nodes represent external tables that exist in a data warehouse or database. Of course, those tables are not under the management
of the DJ server and are often the result of upstream data pipelines. This means changes to those tables can happen at any moment such as columns
being dropped or renamed, types being changed, or entire tables being dropped, renamed, or moved. It's important that DJ is aware of these changes
so that it can understand the effects to downstream nodes. Instead of generating queries based on false assumptions of external tables, live table
reflection allows the system to tell you explicitly when a change in a source node (triggered by a change to an external table) has caused parts of
the DJ DAG to become invalid.

---

## Schema Changes to External Tables

When an external table's schema is changed, reflection will update the corresponding source node. For example, if a `user_id` column is dropped
from an external table, it will also be dropped from the source node. This change will propagate at the column level, meaning any downstream
transform node or dimension node that use the `user_id` column will be marked as invalid. Requesting SQL or data for metrics that use that specific
column will fail in the same way a typical query to a database will fail, specifically mentioning the missing column. Similarly, if a column is added
to an external table, it will also be added to the source node and any downstream nodes will be notified. If nodes exist that were invalid only due to
a broken reference to the added column, they'll now be marked as valid.

[Diagram showing external table changes propagating node status changes throughout the DAG]

---

## DataJunction Reflection Service (DJRS)

Using reflection is recommended but not required. DJ uses an OpenAPI specification based plugin design where a decoupled reflection service can be
utilized by the DJ server. The DJ project includes an open source reflection service [DJRS](https://github.com/DataJunction/djrs) that leverages
optional endpoints on a query service. The open source query service [DJQS](https://github.com/DataJunction/djqs) implements these optional endpoints,
however if you're running a custom query service, these endpoints are only necessary if you want to enable reflection. To learn more, check out the
deployment guides for the [Reflection Service](../deploying-dj/reflection-service/) and
[Query Service](../deploying-dj/query-service/).
