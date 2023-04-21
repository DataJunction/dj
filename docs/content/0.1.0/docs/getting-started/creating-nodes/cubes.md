---
weight: 40
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

Cube nodes can be created by making a `POST` request to `/nodes/cube/`.

{{< tabs "creating cube nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/cube/ \
-H 'Content-Type: application/json' \
-d '{
    "metrics": [
        "num_repair_orders",
        "avg_repair_price",
        "total_repair_cost"
    ],
    "dimensions": [
        "hard_hat.country",
        "hard_hat.postal_code",
        "hard_hat.city",
        "hard_hat.state",
        "dispatcher.company_name",
        "municipality_dim.local_region"
    ],
    "filters": ["hard_hat.state='"'"'AZ'"'"'"]
    "description": "Cube of various metrics related to repairs",
    "mode": "published",
    "display_name": "Repairs Cube",
    "name": "repairs_cube"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
cube = dj.new_cube(
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
)
cube.save()
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
http://localhost:8000/nodes/repairs_cube/materialization/ \
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
