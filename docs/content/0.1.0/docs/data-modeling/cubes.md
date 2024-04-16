---
weight: 8
title: "Cubes"
mermaid: true
---

Cubes are used to represent a set of metrics with dimensions and filters.


| Attribute    | Description                                                                                 | Type         |
|--------------|---------------------------------------------------------------------------------------------|--------------|
| name         | Unique name used by other nodes to select from this node                                    | string       |
| display_name | A human readable name for the node                                                          | string       |
| description  | A human readable description of the node                                                    | string       |
| mode         | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string       |
| metrics      | A set of metric node names                                                                  | list[string] |
| dimensions   | A set of dimension attribute names                                                          | list[string] |
| filters      | A set of filters                                                                            | list[string] |

## Creating Cube Nodes

{{< tabs "creating cube nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/cube/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.repairs_cube",
    "mode": "published",
    "display_name": "Repairs for each company",
    "description": "Cube of the number of repair orders grouped by dispatcher companies",
    "metrics": [
        "default.num_repair_orders"
    ],
    "dimensions": [
        "default.all_dispatchers.company_name"
    ],
    "filters": ["default.all_dispatchers.company_name IS NOT NULL"],
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJBuilder, NodeMode
dj = DJBuilder(DJ_URL)

repairs_cube = dj.create_cube(
    name="repairs_cube",
    display_name="Repairs Cube",
    description="Cube of various metrics related to repairs",
    mode=NodeMode.PUBLISHED,  # for draft nodes, use `mode=NodeMode.DRAFT`
    metrics=[
        "num_repair_orders",
        "avg_repair_price",
        "total_repair_cost"
    ],
    dimensions=[
        "hard_hat.country",
        "hard_hat.postal_code",
        "hard_hat.city",
        "hard_hat.state",
        "dispatcher.company_name",
        "municipality_dim.local_region"
    ],
    filters=["hard_hat.state='AZ'"]
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.cubes.create(
    {
        name: "default.repairs_cube",
        mode: "published",
        display_name: "Repairs for each company",
        description: "Cube of the number of repair orders grouped by dispatcher companies",
        metrics: [
            "default.num_repair_orders"
        ],
        dimensions: [
            "default.all_dispatchers.company_name"
        ],
        filters: ["default.all_dispatchers.company_name IS NOT NULL"]
    }
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

## Adding Materialization Config

Any non-source node in DJ can have user-configurable materialization settings, but for cube nodes, DJ
will seed the node with a set of generic cube materialization settings that can be used downstream by
different materialization engines. Like all other non-source nodes, users can then set engine-specific
materialization config, which will be layered on top of the generic cube materialization settings.

DJ currently supports materialization of cubes into Druid.

This can be added using the following request, assuming that the Druid engine is already configured in
your DJ setup:
{{< tabs "adding materialization" >}}
{{< tab "curl" >}}
```sh
curl -X POST \
http://localhost:8000/nodes/default.repairs_cube/materialization/ \
-H 'Content-Type: application/json'
-d '{
  "engine": {
    "name": "DRUID",
    "version": ""
  },
  "schedule": "0 * * * *",
  "config": {
    "spark": {
      "spark.driver.memory": "4g",
      "spark.executor.memory": "6g"
    },
    "druid": {
      "timestamp_column": "dateint",
      "intervals": ["2023-01-01/2023-03-31"],
      "granularity": "DAY"
    }
  }
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import MaterializationConfig, Engine

config = MaterializationConfig(
    engine=Engine(
        name="DRUID",
        version="",
    ),
    schedule="0 * * * *",
    config={
        "spark": {
            "spark.driver.memory": "4g",
            "spark.executor.memory": "6g",
            "spark.executor.cores": "2",
            "spark.memory.fraction": "0.3",
        },
        "druid": {
            "timestamp_column": "dateint",
            "intervals": ["2023-01-01/2023-03-31"],
            "granularity": "DAY",
        },
    },
)
cube.add_materialization(config)
```
{{< /tab >}}
{{< /tabs >}}
