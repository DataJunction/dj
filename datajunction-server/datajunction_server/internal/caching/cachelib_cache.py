"""
Cachelib-based cache implementation
"""
from typing import Any, Optional

from cachelib import SimpleCache

from datajunction_server.internal.caching.interface import Cache, CacheInterface


class CachelibCache(Cache):
    """A standard implementation of CacheInterface that uses cachelib"""

    def __init__(self):
        super().__init__()
        self.cache = SimpleCache()

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value from the simple cache"""
        return self.cache.get(key)

    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Cache a value in the simple cache"""
        self.cache.set(key, value, timeout=timeout)


def get_cache() -> CacheInterface:
    """Dependency for retrieving a cachelib-based cache implementation"""
    return CachelibCache()
