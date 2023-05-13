---
weight: 50
mermaid: true
---

# Cubes

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
dj.new_cube(
    name="repairs_cube",
    display_name="Repairs Cube",
    description="Cube of various metrics related to repairs",
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
).save()
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

Any non-source node in DJ can have materialization config settings, 
which allows users to set engine-specific materialization config as
well as a cron schedule to materialize the node at.

This can be added using the following request:
{{< tabs "connecting dimension" >}}
{{< tab "curl" >}}
```sh
curl -X POST \
http://localhost:8000/nodes/default.repairs_cube/materialization/ \
-H 'Content-Type: application/json'
-d '{
  "engine_name": "SPARK",
  "engine_version": "3.3",
  "schedule": "0 * * * *",
  "config": {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "6g"
  }
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import MaterializationConfig

config = MaterializationConfig(
    engine_name="SPARK",
    engine_version="3.3",
    schedule="0 * * * *",
    config={
        "spark.driver.memory": "4g",
        "spark.executor.memory": "6g",
        # ...
    }
)
cube.add_materialization_config(config)
```
{{< /tab >}}
{{< /tabs >}}
