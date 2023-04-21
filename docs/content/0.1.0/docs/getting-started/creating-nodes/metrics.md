---
weight: 40
---

# Metrics

Metric nodes represent an aggregation of a measure defined as a single expression in a query that selects from
a single source, transform, or dimension node.

| Attribute    | Description                                                                                 | Type   |
|--------------|---------------------------------------------------------------------------------------------|--------|
| name         | Unique name used by other nodes to select from this node                                    | string |
| display_name | A human readable name for the node                                                          | string |
| description  | A human readable description of the node                                                    | string |
| mode         | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| query        | A SQL query that selects a single expression from a single node                             | string |

## Creating Metric Nodes

Metric nodes can be created by making a `POST` request to `/nodes/metric/`.

{{< tabs "creating metric nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "num_repair_orders"
    "description": "Number of repair orders",
    "mode": "published",
    "query": "SELECT count(repair_order_id) as num_repair_orders FROM repair_orders"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient, NodeMode

dj = DJClient("http://localhost:8000/")
metric = dj.new_metric(
    name="num_repair_orders",
    description="Number of repair orders",
    query="SELECT count(repair_order_id) as num_repair_orders FROM repair_orders",
)
metric.save(NodeMode.PUBLISHED)
```
{{< /tab >}}
{{< /tabs >}}
