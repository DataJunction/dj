"""
Tests for noop cache
"""

from datajunction_server.internal.caching.noop_cache import NoOpCache


def test_noop_cache():
    """
    Test getting, setting, and deleting using a NoOpCache implementation
    """
    cache = NoOpCache()
    assert cache.set(key="foo", value="bar") is None
    assert (
        cache.get(key="foo") is None
    )  # Returns None because NoOp doesn't actually cache
    assert cache.delete(key="foo") is None
