# DataJunction Python Client

## Installation
To install:
```
pip install djclient
```

## Examples

To initialize the client:
```python
from djclient import DJClient
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
from djclient import Catalog
catalog = Catalog(
    name="prod"
)
catalog.publish()
```

To create an engine:
```python
from djclient import Engine
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

All nodes can be found with:
```python
dj.nodes()
```

Specific node types can be retrieved with:
```python
dj.sources()
dj.dimensions()
dj.metrics()
dj.transforms()
dj.cubes()
```

To create a source node:
```python
from djclient import Source
repair_orders = Source(
    name="repair_orders",
    display_name="Repair Orders",
    description="Repair orders",
    catalog="dj",
    schema_="roads",
    table="repair_orders",
)
repair_orders.publish()
```

Nodes can also be created as drafts with:
```python
repair_orders.draft()
```

To create a dimension node:
```python
from djclient import Dimension
repair_order = Dimension(
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
)
repair_order.publish()
```

To create a metric:
```python
from djclient import Metric
num_repair_orders = Metric(
    name="num_repair_orders",
    query="""
    SELECT
      count(repair_order_id) AS num_repair_orders
    FROM repair_orders
    """,
    description="Number of repair orders",
)
num_repair_orders.publish()
```
