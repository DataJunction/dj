---
weight: 10
---

# Sources

Source nodes represent external tables in a database or data warehouse and make up the foundational layer on which other nodes
are built upon.

| Attribute    | Description                                                                                 | Type   |
|--------------|---------------------------------------------------------------------------------------------|--------|
| name         | Unique name used by other nodes to select from this node                                    | string |
| description  | A human readable description of the node                                                    | string |
| display_name | A human readable name for the node                                                          | string |
| mode         | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| catalog      | The name of the external catalog                                                            | string |
| schema_      | The name of the external schema                                                             | string |
| table        | The name of the external table                                                              | string |
| columns      | A map of the external table's column names and types                                        | map    |

## Creating Source Nodes

{{< tabs "creating source nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "repair_orders",
    "description": "Repair orders",
    "mode": "published",
    "catalog": "default",
    "schema_": "roads",
    "table": "repair_orders",
    "columns": [
        {"name": "repair_order_id", "type": "int"},
        {"name": "municipality_id", "type": "string"},
        {"name": "hard_hat_id", "type": "int"},
        {"name": "order_date", "type": "timestamp"},
        {"name": "required_date", "type": "timestamp"},
        {"name": "dispatched_date", "type": "timestamp"},
        {"name": "dispatcher_id", "type": "int"}
    ]
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient, NodeMode

dj = DJClient("http://localhost:8000/")
source = dj.new_source(
    name="repair_orders",
    description="Repair orders",
    catalog="default",
    schema_="roads",
    table="repair_orders",
    columns=[
        {"name": "repair_order_id", "type": "int"},
        {"name": "municipality_id", "type": "string"},
        {"name": "hard_hat_id", "type": "int"},
        {"name": "order_date", "type": "timestamp"},
        {"name": "required_date", "type": "timestamp"},
        {"name": "dispatched_date", "type": "timestamp"},
        {"name": "dispatcher_id", "type": "int"},
    ],
)
source.save(mode=NodeMode.PUBLISHED)
```
{{< /tab >}}
{{< /tabs >}}

## Automatic Column Reflection Using DJRS

To make creating and maintaining source nodes easier, you can optionally run a DataJunction Reflection Service (DJRS). A reflection
service automatically adds the column names and types to all source nodes and updates them whenever the external table changes. This allows
you to exclude the columns when adding a source node.

{{< tabs "creating source nodes with DJRS" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "repair_orders",
    "description": "Repair orders",
    "mode": "published",
    "catalog": "default",
    "schema_": "roads",
    "table": "repair_orders"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient, NodeMode

dj = DJClient("http://localhost:8000/")
source = dj.new_source(
    name="repair_orders",
    description="Repair orders",
    catalog="default",
    schema_="roads",
    table="repair_orders",
)
source.save(NodeMode.PUBLISHED)
```
{{< /tab >}}
{{< /tabs >}}

To read more about DJRS, see the [Table Reflection](../../../dj-concepts/table-reflection/) page.
