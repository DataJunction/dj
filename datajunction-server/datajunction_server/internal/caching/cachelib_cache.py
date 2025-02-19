"""
Cachelib-based cache implementation
"""

from typing import Any, Optional

from cachelib import SimpleCache
from fastapi import Request

from datajunction_server.internal.caching.interface import Cache, CacheInterface
from datajunction_server.internal.caching.noop_cache import noop_cache


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


cachelib_cache = CachelibCache()


def get_cache(request: Request) -> Optional[CacheInterface]:
    """Dependency for retrieving a cachelib-based cache implementation"""
    cache_control = request.headers.get("Cache-Control", "")
    skip_cache = "no-cache" in cache_control
    return noop_cache if skip_cache else cachelib_cache
