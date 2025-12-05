"""
Tests for cache manager
"""

from fastapi import BackgroundTasks
import pytest
from datajunction_server.internal.caching.cachelib_cache import CachelibCache
from datajunction_server.internal.caching.cache_manager import (
    FunctionalRefreshAheadCache,
    RefreshAheadCacheManager,
)
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


# =============================================================================
# Tests for FunctionalRefreshAheadCache
# =============================================================================


async def example_fallback_fn(request, params):
    """Example fallback function for testing."""
    return {"fresh": True, "params": params}


@pytest.mark.asyncio
async def test_functional_cache_basic_usage():
    """
    FunctionalRefreshAheadCache should work with a simple fallback function.
    """
    cache = CachelibCache()
    cm = FunctionalRefreshAheadCache(
        cache=cache,
        fallback_fn=example_fallback_fn,
        cache_key_prefix="test_functional",
        timeout=3600,
    )

    request = DummyRequest()
    background = BackgroundTasks()
    params = {"dimension_name": "test_dim", "node_types": ["metric"]}

    # First call - cache miss, should call fallback
    result = await cm.get_or_load(background, request, params)
    assert result["fresh"] is True
    assert result["params"] == params

    # Run background tasks to store
    for task in background.tasks:
        await task()

    # Now cache should be populated
    key = await cm.build_cache_key(request, params)
    stored = cache.get(key)
    assert stored["fresh"] is True


@pytest.mark.asyncio
async def test_functional_cache_custom_prefix():
    """
    FunctionalRefreshAheadCache should use the provided cache key prefix.
    """
    cache = CachelibCache()
    cm = FunctionalRefreshAheadCache(
        cache=cache,
        fallback_fn=example_fallback_fn,
        cache_key_prefix="dimension_nodes",
        timeout=3600,
    )

    request = DummyRequest()
    key = await cm.build_cache_key(request, {"foo": "bar"})
    assert key.startswith("dimension_nodes:")


@pytest.mark.asyncio
async def test_functional_cache_custom_timeout():
    """
    FunctionalRefreshAheadCache should use the provided timeout.
    """
    cache = CachelibCache()
    cm = FunctionalRefreshAheadCache(
        cache=cache,
        fallback_fn=example_fallback_fn,
        cache_key_prefix="test",
        timeout=7200,
    )
    assert cm.default_timeout == 7200


@pytest.mark.asyncio
async def test_functional_cache_refresh_ahead():
    """
    FunctionalRefreshAheadCache should refresh stale values in the background.
    """
    call_count = 0

    async def counting_fallback(request, params):
        nonlocal call_count
        call_count += 1
        return {"call_count": call_count, "params": params}

    cache = CachelibCache()
    cm = FunctionalRefreshAheadCache(
        cache=cache,
        fallback_fn=counting_fallback,
        cache_key_prefix="test_refresh",
        timeout=3600,
    )

    request = DummyRequest()
    params = {"key": "value"}

    # Pre-populate with stale value
    key = await cm.build_cache_key(request, params)
    cache.set(key, {"stale": True})

    background = BackgroundTasks()
    result = await cm.get_or_load(background, request, params)

    # Should return stale value immediately
    assert result == {"stale": True}

    # Fallback should not have been called yet (background task pending)
    assert call_count == 0

    # Run background refresh
    for task in background.tasks:
        await task()

    # Now fallback should have been called
    assert call_count == 1

    # Cache should now have fresh value
    stored = cache.get(key)
    assert stored["call_count"] == 1


@pytest.mark.asyncio
async def test_functional_cache_respects_cache_control():
    """
    FunctionalRefreshAheadCache should respect Cache-Control headers.
    """
    cache = CachelibCache()
    cm = FunctionalRefreshAheadCache(
        cache=cache,
        fallback_fn=example_fallback_fn,
        cache_key_prefix="test_headers",
        timeout=3600,
    )

    params = {"test": "params"}
    request = DummyRequest()

    # Pre-populate cache
    key = await cm.build_cache_key(request, params)
    cache.set(key, {"cached": True})

    # With no-cache, should bypass cached value
    no_cache_request = DummyRequest(cache_control="no-cache")
    background = BackgroundTasks()
    result = await cm.get_or_load(background, no_cache_request, params)
    assert result["fresh"] is True  # Got fresh value, not cached

    # With no-store, should return cached but not update
    cache.set(key, {"cached": True})
    no_store_request = DummyRequest(cache_control="no-store")
    background = BackgroundTasks()
    result = await cm.get_or_load(background, no_store_request, params)
    assert result == {"cached": True}  # Returns cached
    # Background tasks should be empty (no refresh scheduled)
    assert len(background.tasks) == 0
