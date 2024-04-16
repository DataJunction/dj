---
weight: 5
mermaid: true
title: "Dimensions"
---

{{< alert icon="👉" >}}
Dimension nodes can be thought of as **special views**, with the additional ability to configure joins.
{{< /alert >}}

Dimension nodes include a query that can select from any other node to create a representation of a dimension.
They must always have a primary key configured, and can have any number of associated dimensional attributes.

One key feature of dimension nodes is the ability to configure **join links**. Any DJ node can be linked to a dimension
node via two different types of [dimension linking](../dimension-links). These links are used to build out 
the dimensional metadata edges of the [DJ DAG](../../../dj-concepts/the-dj-dag), enabling DJ to find all accessible
dimensions for each metric.

{{< alert icon="📢" >}}
See the section on [Dimension Links](../dimension-links) for more details. This is an important part of DJ to understand.
{{< /alert >}}

Here are a few example dimension nodes:
<!--
  background-color: #ffefd0 !important;
  color: #a96621;-->
{{< mermaid class="bg-light text-center" >}}
%%{init: {  "theme": "base",
'themeVariables': { 'primaryColor': '#ffefd0'}}}%%
erDiagram
   "default.country | Country" {
        id int PK
        name str
        country_code str
        population long
    }
   "default.dispatcher | Dispatcher" {
        dispatcher_id int PK
        company_name str
        phone str
    }
   "default.contractor | Contractor" {
        contractor_id int PK
        company_name str
        contact_name str
        contact_title str
        address str
        city str
        state str
        postal_code str
        country str
    }
{{< /mermaid >}}

## Creating Dimension Nodes

| Attribute     | Description                                                                                 | Type   |
|---------------|---------------------------------------------------------------------------------------------|--------|
| name          | Unique name used by other nodes to select from this node                                    | string |
| display_name  | A human readable name for the node                                                          | string |
| description   | A human readable description of the node                                                    | string |
| mode          | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| query         | A SQL query that selects from other nodes                                                   | string |

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
from datajunction import DJBuilder, NodeMode
dj = DJBuilder(DJ_URL)

contractor = dj.create_dimension(
    description="Contractor dimension",
    display_name="Default: Contractor",
    name="default.contractor",
    primary_key=['contractor_id'],
    mode=NodeMode.PUBLISHED,  # for draft nodes, use `mode=NodeMode.DRAFT`
    query="""
    SELECT
        contractor_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        state,
        postal_code,
        country
    FROM default.contractors
    """
)
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
