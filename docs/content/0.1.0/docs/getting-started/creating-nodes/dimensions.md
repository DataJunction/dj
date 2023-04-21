---
weight: 40
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

Dimension nodes can be created by making a `POST` request to `/nodes/dimension/`.

{{< tabs "creating dimension nodes" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "us_state",
    "description": "US state dimension",
    "mode": "published",
    "query": """
        SELECT
        state_id,
        state_name,
        state_abbr,
        state_region,
        r.us_region_description AS state_region_description
        FROM us_states s
        LEFT JOIN us_region r
        ON s.state_region = r.us_region_id
    """
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient, NodeMode

dj = DJClient("http://localhost:8000/")
dimension = dj.new_dimension(
    name="us_state",
    description="US state dimension",
    query="""
        SELECT
        state_id,
        state_name,
        state_abbr,
        state_region,
        r.us_region_description AS state_region_description
        FROM us_states s
        LEFT JOIN us_region r
        ON s.state_region = r.us_region_id
    """,
)
dimension.save(NodeMode.PUBLISHED)
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
curl -X POST \
http://localhost:8000/nodes/hard_hats/columns/hard_hat_id/?dimension=hard_hat_state&dimension_column=hard_hat_id \
-H 'Content-Type: application/json'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
dimension = dj.dimension("hard_hats")
dimension.link_dimension(
    column="hard_hat_id",
    dimension="hard_hat_state",
    dimension_column="hard_hat_id",
)
```
{{< /tab >}}
{{< /tabs >}}
