---
weight: 40
---

# Transforms

Transform nodes allow you to do arbitray SQL operations on sources, dimensions, and even other transform nodes. Of course with
a perfect data model, you may not need to define any transform nodes. However, in some cases it may be convenient to use transform
nodes to clean up your external data within DJ by joining, aggregating, casting types, or any other SQL operation that your query
engine supports.

| Attribute    | Description                                                                                 | Type   |
|--------------|---------------------------------------------------------------------------------------------|--------|
| name         | Unique name used by other nodes to select from this node                                    | string |
| display_name | A human readable name for the node                                                          | string |
| description  | A human readable description of the node                                                    | string |
| mode         | `published` or `draft` (see [Node Mode](../../../dj-concepts/node-dependencies/#node-mode)) | string |
| query        | A SQL query that selects from other nodes                                                   | string |

## Creating Transform Nodes

{{< tabs "creating transform nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/transform/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "repair_orders_w_dispatchers",
    "description": "Repair orders that have a dispatcher",
    "mode": "published",
    "query": """
        SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        dispatcher_id
        FROM repair_orders
        WHERE dispatcher_id IS NOT NULL
    """
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient, NodeMode

dj = DJClient("http://localhost:8000/")
transform = dj.new_transform(
    name="repair_orders_w_dispatchers",
    description="Repair orders that have a dispatcher",
    query="""
        SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        dispatcher_id
        FROM repair_orders
        WHERE dispatcher_id IS NOT NULL
    """,
)
transform.save(NodeMode.PUBLISHED)
```
{{< /tab >}}
{{< /tabs >}}
