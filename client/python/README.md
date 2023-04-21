# DataJunction Python Client

## Installation
To install:
```
pip install datajunction
```

## Examples

To initialize the client:

```python
from datajunction import DJClient

dj = DJClient("http://dj-endpoint:8000")
```

### Catalogs and Engines

To list available catalogs for the DJ host:
```python
dj.catalogs()
```

To list available engines for the DJ host:
```python
dj.engines()
```

To create a catalog:

```python
from datajunction import Catalog

catalog = Catalog(
    name="prod"
)
catalog.publish()
```

To create an engine:

```python
from datajunction import Engine

engine = Engine(
    name="spark",
    version="3.2.2",
    uri="..."
)
engine.publish()
```

To attach an engine to a catalog:
```python
catalog.add_engine(engine)
```

### Nodes

All nodes for a given namespace can be found with:
```python
dj.namespace("default").nodes()
```

Specific node types can be retrieved with:
```python
dj.namespace("default").sources()
dj.namespace("default").dimensions()
dj.namespace("default").metrics()
dj.namespace("default").transforms()
dj.namespace("default").cubes()
```

To create a source node:

```python
from datajunction import NodeMode

repair_orders = dj.new_source(
    name="repair_orders",
    display_name="Repair Orders",
    description="Repair orders",
    catalog="dj",
    schema_="roads",
    table="repair_orders",
)
repair_orders.save(mode=NodeMode.PUBLISHED)
```

Nodes can also be created as drafts with:
```python
repair_orders.save(mode=NodeMode.DRAFT)
```

To create a dimension node:
```python
repair_order = dj.new_dimension(
    name="repair_order",
    query="""
    SELECT
      repair_order_id,
      municipality_id,
      hard_hat_id,
      dispatcher_id
    FROM repair_orders
    """,
    description="Repair order dimension",
    primary_key=["repair_order_id"],
)
repair_order.save()
```

To create a transform node:
```python
large_revenue_payments_only = dj.new_transform(
    name="large_revenue_payments_only",
    query="""
    SELECT
      payment_id,
      payment_amount,
      customer_id,
      account_type
    FROM revenue
    WHERE payment_amount > 1000000
    """,
    description="Only large revenue payments",
)
large_revenue_payments_only.save()
```

To create a metric:
```python
num_repair_orders = dj.new_metric(
    name="num_repair_orders",
    query="""
    SELECT
      count(repair_order_id) AS num_repair_orders
    FROM repair_orders
    """,
    description="Number of repair orders",
)
num_repair_orders.save()
```
