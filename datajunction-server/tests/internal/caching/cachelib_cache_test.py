"""
Tests for cachelib cache implementation
"""

from starlette.requests import Headers, Request

from datajunction_server.internal.caching.cachelib_cache import CachelibCache, get_cache
from datajunction_server.internal.caching.noop_cache import NoOpCache


def test_cachelib_cache():
    """
    Test getting, setting, and deleting using the cachelib implementation
    """
    cache = CachelibCache()
    assert cache.set(key="foo", value="bar", timeout=300) is None
    assert cache.get(key="foo") == "bar"
    assert cache.delete(key="foo") is None
    assert cache.get(key="foo") is None


def test_cachelib_cache_nocache_headers():
    """Test cachelib cache with various request headers"""

    # Test with "no-cache" in headers
    nocache_request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": Headers({"Cache-Control": "no-cache"}).raw,
            "query_string": b"",
        },
    )
    get_cache(nocache_request)
    assert isinstance(get_cache(nocache_request), NoOpCache)

    # Test without "no-cache" in headers
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": Headers({"Cache-Control": "max-age=3600"}).raw,
            "query_string": b"",
        },
    )
    get_cache(request)
    assert isinstance(get_cache(request), CachelibCache)

    # Test with no headers at all
    headerless_request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
            "query_string": b"",
        },
    )

    assert isinstance(get_cache(headerless_request), CachelibCache)
