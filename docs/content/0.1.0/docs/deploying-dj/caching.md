---
weight: 50
title: Caching
---

In DataJunction, caching is a crucial component that helps optimize performance by storing and reusing results of expensive operations, such as computing the dimension DAG (Directed Acyclic Graph). This section discusses how caching is used within DataJunction and how you can implement a custom caching solution using `fastapi-cache`.

### How Caching is Used

DataJunction employs caching in multiple areas to enhance performance and reduce the load on the database. One of the primary use cases is caching the results of expensive operations like computing the dimension DAG. By caching these results, DataJunction can quickly return previously computed results without having to recompute them, thereby saving time and resources.

### Default Caching Implementation

Out of the box, DataJunction uses the in-memory backend that comes packaged with `fastapi-cache`. This library is straightforward and efficient for development and small-scale deployments, however `fastapi-cache` comes with other backend implementations, such as Redis, and even makes it easy to use your own custom caching implementation.

### Custom Caching Implementation

You can implement a custom cache by using `fastapi-cache`'s `Backend` interface. The custom cache must implement methods such as `get`, `set`, and `delete`.

For detailed information on how to implement a custom `Backend`, please refer to the [fastapi-cache documentation](https://github.com/long2ice/fastapi-cache).

### Respecting the `no-cache` Header

The `fastapi-cache` library respects the `no-cache` header in requests. This means that if a request contains the `Cache-Control: no-cache` header, the cache will be bypassed, and fresh data will be fetched.

For more information on how to configure and use `fastapi-cache`, please refer to the [fastapi-cache documentation](https://github.com/long2ice/fastapi-cache).
