---
weight: 30
mermaid: true
---

# Dimensions

Dimension nodes are critical for defining the cross edges of the [DJ DAG](../../../dj-concepts/the-dj-dag) and are instrumental in
many of DJ's core features. They include a query that can select from any other node to create a representation of a dimension. Any
column in any DJ node can then be tagged as a join key to any column on the dimension node. These join paths are used by DJ to
discover all dimensions that are accessible for each metric. Some dimensions themselves may include join keys to other dimensions,
further extending the join path and allowing DJ to discover more dimensions that can be used to group metrics.

| Attribute     | Description                                                                                 | Type   |
|---------------|---------------------------------------------------------------------------------------------|--------|
| name          | Unique name used by other nodes to select from this node                                    | string |
| display_name  | A human readable name for the node                                                          | string |
| description   | A human readable description of the node                                                    | string |
| mode          | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| query         | A SQL query that selects from other nodes                                                   | string |

## Creating Dimension Nodes

Assume a `default.dispatchers` source node was defined as follows.

{{< tabs "creating source node for dimension node" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/nodes/source/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
        "columns": [
            {"name": "dispatcher_id", "type": "int"},
            {"name": "company_name", "type": "string"},
            {"name": "phone", "type": "string"}
        ],
        "description": "Contact list for dispatchers",
        "mode": "published",
        "name": "default.dispatchers",
        "catalog": "warehouse",
        "schema_": "roads",
        "table": "dispatchers"
  }'
```
{{< /tab >}}
{{< tab "python" >}}
```py

```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.sources.create(
  {
    name: "default.dispatchers",
    mode: "published",
    description: "Contact list for dispatchers",
    catalog: "warehouse",
    schema_: "roads",
    table: "dispatchers",
    columns: [
        {name: "dispatcher_id", type: "int"},
        {name: "company_name", type: "string"},
        {name: "phone", type: "string"}
    ]
  }
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

You can then define a dimension node that selects from the `default.dispatchers` source node and includes
a primary key. Let's call it `default.dispatcher`.
{{< tabs "creating a dimension node" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.all_dispatchers",
    "description": "All dispatchers",
    "mode": "published",
    "query": "SELECT dispatcher_id, company_name, phone FROM default.dispatchers",
    "primary_key": ["dispatcher_id"]
}'
```
{{< /tab >}}
{{< tab "python" >}}
```py

```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.dimensions.create(
  {
    name: "default.all_dispatchers",
    mode: "published",
    description: "All dispatchers",
    query: `
        SELECT
        dispatcher_id,
        company_name,
        phone
        FROM default.dispatchers
    `,
    primary_key: ["dispatcher_id"]
  }
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

## Connecting Node Columns to Dimensions

Any column on any node can be identified as a join key to a dimension node column by making
a `POST` request to the columns. For example, let's assume you have a `hard_hats` node that contains
employee information. The state in which the employee works is stored in a separate lookup table
that includes a mapping of `hard_hat_id` to `state_id`.

{{< excalidraw connecting_a_dimension >}}

This connection in DJ can be added using the following request.
{{< tabs "connecting dimension" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/nodes/default.repair_orders/columns/dispatcher_id/?dimension=default.all_dispatchers&dimension_column=dispatcher_id' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "python" >}}

```py
dimension = dj.dimension("default.repair_orders")
dimension.link_dimension(
    column="dispatcher_id",
    dimension="default.dispatcher",
    dimension_column="dispatcher_id",
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.dimensions.link("default.repair_orders", "dispatcher_id", "default.all_dispatchers", "dispatcher_id").then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}
