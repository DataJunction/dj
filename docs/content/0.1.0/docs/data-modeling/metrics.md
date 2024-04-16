---
weight: 7
title: "Metrics"
---

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

{{< tabs "creating metric nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.num_repair_orders",
    "description": "Number of repair orders",
    "mode": "published",
    "query": "SELECT count(repair_order_id) as num_repair_orders FROM default.repair_orders"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJBuilder, NodeMode
dj = DJBuilder(DJ_URL)

metric = dj.create_metric(
    name="default.num_repair_orders",
    description="Number of repair orders",
    query="SELECT count(repair_order_id) FROM repair_orders",
    mode=NodeMode.PUBLISHED,  # for draft nodes, use `mode=NodeMode.DRAFT`
)
print(metric.name)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.metrics.create(
    {
        name: "default.num_repair_orders",
        description: "Number of repair orders",
        mode: "published",
        query: `
            SELECT
            count(repair_order_id) as num_repair_orders
            FROM default.repair_orders
        `
    }
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}
