---
weight: 10
title: "Sources"
---

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
automatically inherit that source node's catalog. Here's an example of creating a `duckdb` engine and a `warehouse` catalog.

### Create a Catalog

{{< tabs "creating a catalog" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/catalogs/ \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "warehouse"
}'
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.catalogs.create({"name": "warehouse"}).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

### Create an Engine

{{< tabs "creating an engine" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "duckdb",
  "version": "0.7.1",
  "dialect": "spark"
}'
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.engines.create({
    name: "duckdb",
    version: "0.7.1",
    dialect: "spark",
}).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

### Attaching Existing Engines and Catalogs

Once a catalog and engine defintion are created, the engine definition can be attached to the catalog.
Here is an example of attaching the existing `duckdb` engine definition to the existing `warehouse` catalog.

{{< tabs "attaching an engine" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/catalogs/warehouse/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '[{
  "name": "duckdb",
  "version": "0.7.1"
}]'
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.catalogs.addEngine("warehouse", "duckdb", "0.7.1").then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

## Registering Tables as Source Nodes

Assuming that you've configured a reflection service and query service to interface with your data 
warehouse setup, you can just register a table as a source node by telling DJ about the table. 
The reflection service will automatically add the column names and types to all source nodes and 
update them whenever the external table changes.

To read more about DJRS, see the [Table Reflection](../../../dj-concepts/table-reflection/) page.

{{< tabs "creating source nodes with DJRS" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000//register/table/warehouse/roads/repair_orders/
```
{{< /tab >}}
{{< tab "python" >}}

```py
table = dj.register_table(
    catalog="warehouse",
    schema_="roads",
    table="repair_orders",
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
```
{{< /tab >}}
{{< /tabs >}}

## Create Source Node Manually
{{< tabs "creating source nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.repair_orders",
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
source = dj.create_source(
    name="default.repair_orders",
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
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.sources.create({
    name: "default.repair_orders",
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
}).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}
