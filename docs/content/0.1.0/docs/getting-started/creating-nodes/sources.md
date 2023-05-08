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

## Creating Catalogs and Engines

Before creating source nodes, a DataJunction server must contain at least one catalog. A catalog represents a catalog in
your external data warehouse and includes one or more engine definitions. All nodes that select from a source node,
automatically inherit that source node's catalog. Here's an example of creating a `spark` engine and a `warehouse` catalog.

### Create an Engine

{{< tabs "creating an engine" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "spark",
  "version": "3.1.1",
  "dialect": "spark"
}'
```
{{< /tab >}}
{{< /tabs >}}

### Create a Catalog

{{< tabs "creating a catalog" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/catalogs/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "warehouse",
  "engines": [{"name": "spark", "version": "3.1.1"}]
}'
```
{{< /tab >}}
{{< /tabs >}}

### Attaching Existing Engines and Catalogs

If a catalog and engine defintion already exist, the engine definition can be attached to the catalog.
Here is an example of attaching an existing `spark` engine definition to an existing `warehouse` catalog.

{{< tabs "attaching an engine" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/catalogs/warehouse/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '[
  {
    "name": "spark",
    "version": "3.1.1"
  }
]'
```
{{< /tab >}}
{{< /tabs >}}

## Creating a Source Node

{{< tabs "creating source nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "warehouse.repair_orders",
    "description": "Repair orders",
    "mode": "published",
    "catalog": "warehouse",
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
    name="warehouse.repair_orders",
    description="Repair orders",
    catalog="warehouse",
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
{{< tab "javascript" >}}
```js
const { DJClient } = require('datajunction')

const dj = DJClient('http://localhost:8000/')
dj.sources.create({
    name: "warehouse.repair_orders",
    description: "Repair orders",
    mode: "published",
    catalog: "warehouse",
    schema_: "roads",
    table: "repair_orders",
    columns: [
        {name: "repair_order_id", type: "int"},
        {name: "municipality_id", type: "string"},
        {name: "hard_hat_id", type: "int"},
        {name: "order_date", type: "timestamp"},
        {name: "required_date", type: "timestamp"},
        {name: "dispatched_date", type: "timestamp"},
        {name: "dispatcher_id", type: "int"}
    ]
})
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
    "catalog": "warehouse",
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
    name="warehouse.repair_orders",
    description="Repair orders",
    catalog="warehouse",
    schema_="roads",
    table="repair_orders",
)
source.save(NodeMode.PUBLISHED)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
const { DJClient } = require('datajunction')

const dj = DJClient('http://localhost:8000/')
dj.sources.create(
    {
        name: "warehouse.repair_orders",
        description: "Repair orders",
        mode: "published",
        catalog: "warehouse",
        schema_: "roads",
        table: "repair_orders",
    }
)
```
{{< /tab >}}
{{< /tabs >}}

To read more about DJRS, see the [Table Reflection](../../../dj-concepts/table-reflection/) page.
