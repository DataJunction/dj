# DataJunction Python Client

This is a short introduction into the Python version of the Data Junction (DJ) client.
For a full comprehensive intro into the DJ functionality please check out [datajunction.io](https://datajunction.io/).

## Installation

To install:
```
pip install datajunction
```

## DJ Client : Basic Access

Here you can see how to access and use the most common Data Junction features.

### Examples

To initialize the client:

```python
from datajunction import DJClient

dj = DJClient("http://localhost:8000")
```

**NOTE**
If you are running in our demo docker environment please change the above URL to "http://dj:8000".

You are now connected to your DJ service and you can start looking around. Let's see what namespaces we have in the system:

```python
dj.list_namespaces()

['default']
```

Next let's see what metrics and dimensions exist in the `default` namespace:

```python
dj.list_metrics(namespace="default")

['default.num_repair_orders',
 'default.avg_repair_price',
 'default.total_repair_cost',
 'default.avg_length_of_employment',
 'default.total_repair_order_discounts',
 'default.avg_repair_order_discounts',
 'default.avg_time_to_dispatch']

dj.list_dimensions(namespace="default")

['default.date_dim',
 'default.repair_order',
 'default.contractor',
 'default.hard_hat',
 'default.local_hard_hats',
 'default.us_state',
 'default.dispatcher',
 'default.municipality_dim']
```

Now let's pick two metrics and see what dimensions they have in common:

```python
dj.common_dimensions(
  metrics=["default.num_repair_orders", "default.total_repair_order_discounts"],
  name_only=True
)

['default.dispatcher.company_name',
 'default.dispatcher.dispatcher_id',
 'default.dispatcher.phone',
 'default.hard_hat.address',
 'default.hard_hat.birth_date',
 'default.hard_hat.city',
 ...
```

And finally let's ask DJ to show us some data for these metrics and some dimensions:

```python
dj.data(
    metrics=["default.num_repair_orders", "default.total_repair_order_discounts"],
    dimensions=["default.hard_hat.city"]
)

| default_DOT_num_repair_orders	| default_DOT_total_repair_order_discounts | city        |
| ----------------------------- | ---------------------------------------- | ----------- |
| 4                             |                              5475.110138 | Jersey City |
| 3                             |                             11483.300049 | Billerica   |
| 5	                            |                              6725.170074 | Southgate   |
...
```

### Reference

List of all available DJ client methods:

- DJClient:

  ### list
  - list_namespaces( prefix: Optional[str])
  - list_dimensions( namespace: Optional[str])
  - list_metrics( namespace: Optional[str])
  - list_cubes( namespace: Optional[str])
  - list_sources( namespace: Optional[str])
  - list_transforms( namespace: Optional[str])
  - list_nodes( namespace: Optional[str], type_: Optional[NodeType])

  ### find
  - common_dimensions( metrics: List[str], name_only: bool = False)
  - common_metrics( dimensions: List[str], name_only: bool = False)

  ### execute
  - sql( metrics: List[str],
        dimensions: Optional[List[str]],
        filters: Optional[List[str]],
        engine_name: Optional[str],
        engine_version: Optional[str])
  - data( metrics: List[str],
        dimensions: Optional[List[str]],
        filters: Optional[List[str]],
        engine_name: Optional[str],
        engine_version: Optional[str],
        async_: bool = True)

## DJ Builder : Data Modelling

In this section we'll show you few examples to modify the DJ data model and its nodes.

### Start Here

To initialize the DJ builder:

```python
from datajunction import DJBuilder

djb = DJBuilder("http://localhost:8000")
```

**NOTE**
If you are running in our demo docker container please change the above URL to "http://dj:8000".

### Namespaces

To access a namespace or check if it exists you can use the same simple call:

```python
djb.namespace("default")

Namespace(dj_client=..., namespace='default')
```
```python
djb.namespace("foo")

[DJClientException]: Namespace `foo` does not exist.
```

To create a namespace:

```python
djb.create_namespace("foo")

Namespace(dj_client=..., namespace='foo')
```

To delete (or restore) a namespace:

```python
djb.delete_namespace("foo")

djb.restore_namespace("foo")
```

**NOTE:**
The `cascade` parameter in both of above methods allows for cascading
effect applied to all underlying nodes and namespaces. Use it with caution!

### Nodes

To learn what **Node** means in the context of DJ, please check out [this datajuntion.io page](https://datajunction.io/docs/0.1.0/dj-concepts/nodes/).

To list all (or some) nodes in the system you can use the `list_<node-type>()` methods described
in the **DJ Client : Basic Access** section or you can use the namespace based method:

All nodes for a given namespace can be found with:
```python
djb.namespace("default").nodes()
```

Specific node types can be retrieved with:
```python
djb.namespace("default").sources()
djb.namespace("default").dimensions()
djb.namespace("default").metrics()
djb.namespace("default").transforms()
djb.namespace("default").cubes()
```

To create a source node:

```python
repair_orders = djb.create_source(
    name="repair_orders",
    display_name="Repair Orders",
    description="Repair orders",
    catalog="dj",
    schema_="roads",
    table="repair_orders",
)
```

Nodes can also be created in draft mode:

```python
repair_orders = djb.create_source(
    ...,
    mode=NodeMode.DRAFT
)
```

To create a dimension node:

```python
repair_order = djb.create_dimension(
    name="default.repair_order_dim",
    query="""
    SELECT
      repair_order_id,
      municipality_id,
      hard_hat_id,
      dispatcher_id
    FROM default.repair_orders
    """,
    description="Repair order dimension",
    primary_key=["repair_order_id"],
)
```

To create a transform node:
```python
large_revenue_payments_only = djb.create_transform(
    name="default.large_revenue_payments_only",
    query="""
    SELECT
      payment_id,
      payment_amount,
      customer_id,
      account_type
    FROM default.revenue
    WHERE payment_amount > 1000000
    """,
    description="Only large revenue payments",
)
```

To create a metric:
```python
num_repair_orders = djb.create_metric(
    name="default.num_repair_orders",
    query="""
    SELECT
      count(repair_order_id)
    FROM repair_orders
    """,
    description="Number of repair orders",
)
```

### Reference

List of all available DJ builder methods:

- DJBuilder:

  ### namespaces
  - namespace( namespace: str)
  - create_namespace( namespace: str)
  - delete_namespace(self, namespace: str, cascade: bool = False)
  - restore_namespace(self, namespace: str, cascade: bool = False)

  ### nodes
  - delete_node(self, node_name: str)
  - restore_node(self, node_name: str)

  ### nodes: source
  - source(self, node_name: str)
  - create_source( ..., mode: Optional[NodeMode] = NodeMode.PUBLISHED)
  - register_table( catalog: str, schema: str, table: str)

  ### nodes: transform
  - transform(self, node_name: str)
  - create_transform( ..., mode: Optional[NodeMode] = NodeMode.PUBLISHED)

  ### nodes: dimension
  - dimension(self, node_name: str)
  - create_dimension( ..., mode: Optional[NodeMode] = NodeMode.PUBLISHED)

  ### nodes: metric
  - metric(self, node_name: str)
  - create_metric( ..., mode: Optional[NodeMode] = NodeMode.PUBLISHED)

  ### nodes: cube
  - cube(self, node_name: str)
  - create_cube( ..., mode: Optional[NodeMode] = NodeMode.PUBLISHED)


## DJ System Administration

### Examples

To list available catalogs:

```python
djb.list_catalogs()

['warehouse']
```

To list available engines:

```python
djb.list_engines()

[{'name': 'duckdb', 'version': '0.7.1'}]
```

To create a catalog: (TODO)

```python
from datajunction import Catalog

catalog = Catalog(
    name="prod"
)
catalog.publish()
```

To create an engine: (TODO)

```python
from datajunction import Engine

engine = Engine(
    name="spark",
    version="3.2.2",
    uri="..."
)
engine.publish()
```

To attach an engine to a catalog: (TODO)
```python
catalog.add_engine(engine)
```

### Reference

List of all available DJ builder methods:

- DJBuilder:

  ### catalogs
  - list_catalogs()
  - get_catalog( name: str)
  - add_catalog( data: str) - TODO

  ### catalogs
  - list_engines()
  - get_engine( name: str)
  - add_engine( data: str) - TODO
