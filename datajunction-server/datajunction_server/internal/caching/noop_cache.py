"""
NoOp cache implementation
"""

from datajunction_server.internal.caching.interface import Cache


class NoOpCache(Cache):
    """A NoOp implementation of CacheInterface that returns None for get and set"""


noop_cache = NoOpCache()
