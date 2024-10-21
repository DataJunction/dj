---
weight: 50
title: Caching
---

In DataJunction, caching is a crucial component that helps optimize performance by storing and reusing results of expensive operations, such as computing the dimension DAG (Directed Acyclic Graph). This section discusses how caching is used within DataJunction and how you can implement a custom caching solution using FastAPI's dependency injection.

### How Caching is Used

DataJunction employs caching in multiple areas to enhance performance and reduce the load on the database. One of the primary use cases is caching the results of expensive operations like computing the dimension DAG. By caching these results, DataJunction can quickly return previously computed results without having to recompute them, thereby saving time and resources.

### Default Caching Implementation

Out of the box, DataJunction comes with a simple in-memory cache that uses `SimpleCache` from the `cachelib` library. This implementation is straightforward and efficient for development and small-scale deployments.

Here's a brief look at the default caching implementation:

```py
from cachelib import SimpleCache

class CachelibCache(Cache):
    """A standard implementation of CacheInterface that uses cachelib"""

    def __init__(self):
        super().__init__()
        self.cache = SimpleCache()

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value from the simple cache"""
        super().get(key)
        return self.cache.get(key)

    def set(self, key: str, value: Any, timeout: int = 3600) -> None:
        """Cache a value in the simple cache"""
        super().set(key, value, timeout)
        self.cache.set(key, value, timeout=timeout)

    def delete(self, key: str) -> None:
        """Delete a key in the simple cache"""
        super().delete(key)
        self.cache.delete(key)
```

### Custom Caching Implementation

You can implement a custom cache by using FastAPI's dependency injection and injecting a `get_cache` dependency.
The custom cache must implement the `CacheInterface`, which includes the `get` and `set` methods.

Here's the `CacheInterface` definition:

```py
from abc import ABC, abstractmethod
from typing import Any, Optional

class CacheInterface(ABC):
    """Cache interface"""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get a cached value"""

    @abstractmethod
    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Cache a value"""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a cache key"""
```

#### Implementing a Custom Cache

To implement a custom cache, create a class that extends `CacheInterface` and override the `get` and `set` methods. Then, use FastAPI's dependency injection to inject your custom cache.

Here's an example of a custom cache implementation:

```py
from fastapi import Request
from datajunction_server.internal.caching.noop_cache import noop_cache

class MyCustomCache(CacheInterface):
    """A custom cache implementation"""

    def __init__(self):
        # Initialize your custom cache here
        ...

    def get(self, key: str) -> Optional[Any]:
        # Implement the logic to retrieve a cached value
        ...

    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        # Implement the logic to cache a value
        ...

    def delete(self, key: str) -> None:
        # Implement the logic to delete a cache key
        ...

def get_cache(request: Request) -> Optional[CacheInterface]:
    """Dependency for retrieving a custom cache implementation"""
    cache_control = request.headers.get("Cache-Control", "")
    skip_cache = "no-cache" in cache_control
    return noop_cache if skip_cache else MyCustomCache()
```

### Respecting the `no-cache` Header

The open-source `get_cache` dependency respects the `no-cache` header in requests. This means that if a request contains
the `Cache-Control: no-cache` header, the cache will be bypassed, and fresh data will be fetched. This is done by
returning an instance of `NoOpCache` which simply wraps the base `Cache` implementation that logs caching activity.
It is recommended that custom cache implementations also respect this header to ensure consistency. "Turning off the
cache" when a `no-cache` header is detected is as simple as making sure the dependency injected function returns the
`NoOpCache` instance that can be imported from `datajunction_server.internal.caching.noop_cache`.

Here's the open-source `get_cache` dependency for reference:

```py
def get_cache(request: Request) -> Optional[CacheInterface]:
    """Dependency for retrieving a cachelib-based cache implementation"""
    cache_control = request.headers.get("Cache-Control", "")
    skip_cache = "no-cache" in cache_control
    return noop_cache if skip_cache else cachelib_cache
```