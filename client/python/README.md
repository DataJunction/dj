# DataJunction Python Client

## Installation
To install:
```
pip install djclient
```

## Examples

To initialize the client:
```
from djclient import DJClient
client = DJClient("http://dj-endpoint:8000")
```

To list available catalogs for the DJ host:
```
client.catalogs()
```

To list available engines for the DJ host:
```
client.engines()
```

ALl nodes can be found with:
```
client.nodes()
```

Specific node types can be retrieved with:
```
client.sources()
client.dimensions()
client.metrics()
client.transforms()
client.cubes()
```
