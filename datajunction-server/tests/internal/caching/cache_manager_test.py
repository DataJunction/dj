"""
Tests for cache manager
"""

from fastapi import BackgroundTasks
import pytest
from datajunction_server.internal.caching.cachelib_cache import CachelibCache
from datajunction_server.internal.caching.cache_manager import RefreshAheadCacheManager
from starlette.datastructures import Headers


class ExampleCacheManager(RefreshAheadCacheManager):
    async def fallback(self, request, params):
        return {"fresh": True, "params": params}


class DummyRequest:
    """
    Fake request for testing. Allows easy setting of Cache-Control variations.
    """

    def __init__(self, cache_control: str | None = None):
        headers = {}
        if cache_control:
            headers["Cache-Control"] = cache_control
        self.headers = Headers(headers)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cache_control,expect_store,expect_cache_hit",
    [
        ("", True, True),  # default: uses and stores cache
        ("no-cache", True, False),  # bypass cache, but stores fresh value
        ("no-store", False, True),  # bypass AND do not store
        ("no-cache, no-store", False, False),  # both flags: bypass and skip store
    ],
)
async def test_refresh_ahead_cache_headers(
    cache_control,
    expect_store,
    expect_cache_hit,
):
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    params = {"foo": "bar"}
    request = DummyRequest(cache_control=cache_control)
    background = BackgroundTasks()

    # Pre-populate cache to test hit vs miss
    key = await cm.build_cache_key(request, params)
    cache.set(key, {"cached": True})

    result = await cm.get_or_load(background, request, params)

    # If it was a hit, the cached version comes back
    if expect_cache_hit:
        assert result == {"cached": True}
    else:
        assert result["fresh"] is True

    # Run any background tasks (e.g. refresh or store)
    for task in background.tasks:
        await task()

    key = await cm.build_cache_key(request, params)

    # Run background tasks to store if needed
    for task in background.tasks:
        await task()

    # After tasks, see what's in the cache
    stored = cache.get(key)
    if expect_store:
        assert stored == {
            "fresh": True,
            "params": {
                "foo": "bar",
            },
        }
    else:
        assert stored == {"cached": True}


@pytest.mark.asyncio
async def test_build_cache_key_consistency():
    """
    The same params should always produce the same key.
    """
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    params1 = {"a": 1, "b": 2}
    params2 = {"b": 2, "a": 1}

    request = DummyRequest()
    key1 = await cm.build_cache_key(request, params1)
    key2 = await cm.build_cache_key(request, params2)

    assert key1 == key2
    assert key1.startswith("examplecachemanager:")


@pytest.mark.asyncio
async def test_invalid_params_type():
    """
    Using a bad params type should raise TypeError.
    """
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    request = DummyRequest()

    class NotValid:
        pass

    with pytest.raises(TypeError):
        await cm.build_cache_key(request, NotValid())


@pytest.mark.asyncio
async def test_refresh_cache_explicit():
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    request = DummyRequest()
    params = {"x": 1}

    key = await cm.build_cache_key(request, params)

    # Before refresh: nothing
    assert cache.get(key) is None

    # Do refresh
    await cm._refresh_cache(key, request, params)

    # Should now have a fresh value
    stored = cache.get(key)
    assert stored["fresh"] is True
    assert stored["params"] == {"x": 1}


@pytest.mark.asyncio
async def test_cache_key_prefix_override():
    class CustomPrefixManager(ExampleCacheManager):
        _cache_key_prefix = "customprefix"

    cm = CustomPrefixManager(CachelibCache())
    request = DummyRequest()
    key = await cm.build_cache_key(request, {"foo": "bar"})
    assert key.startswith("customprefix:")


@pytest.mark.asyncio
async def test_fallback_runs_on_cache_miss():
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    request = DummyRequest()
    params = {"hello": "world"}
    background = BackgroundTasks()

    result = await cm.get_or_load(background, request, params)

    assert result["fresh"] is True


@pytest.mark.asyncio
async def test_background_refresh_updates_cache():
    cache = CachelibCache()
    cm = ExampleCacheManager(cache)
    params = {"foo": "bar"}
    request = DummyRequest()
    background = BackgroundTasks()

    key = await cm.build_cache_key(request, params)
    # Prepopulate with stale value
    cache.set(key, {"cached": True})

    result = await cm.get_or_load(background, request, params)
    assert result == {"cached": True}

    # Run background refresh tasks
    for task in background.tasks:
        await task()

    # After refresh, the cache should be updated with fresh value
    stored = cache.get(key)
    assert stored["fresh"] is True
