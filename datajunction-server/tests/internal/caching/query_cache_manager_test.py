from unittest.mock import patch

import pytest
from fastapi import BackgroundTasks
from starlette.datastructures import Headers

from datajunction_server.internal.caching.cachelib_cache import CachelibCache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)
from datajunction_server.database.queryrequest import QueryBuildType


class DummyRequest:
    """
    Fake request for testing. Allows easy setting of Cache-Control variations.
    """

    def __init__(self, cache_control: str | None = None):
        headers = {}
        if cache_control:
            headers["Cache-Control"] = cache_control
        self.headers = Headers(headers)
        self.method = "GET"


@pytest.mark.asyncio
async def test_cache_key_prefix_uses_query_type():
    """
    The cache key prefix should include the query type.
    """
    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    assert manager.cache_key_prefix == "sql:measures"


@pytest.mark.asyncio
@patch(
    "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
)
async def test_build_cache_key_calls_versioning(version_query_request_mock):
    """
    Should call version_query_request and build the key.
    """
    version_query_request_mock.return_value = "versioned123"

    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    params = QueryRequestParams(
        query_type=QueryBuildType.MEASURES,
        nodes=["foo"],
        dimensions=["dim1"],
        filters=[],
    )
    request = DummyRequest()
    key = await manager.build_cache_key(request, params)

    version_query_request_mock.assert_called_once()
    assert key.startswith("sql:measures:")


@pytest.mark.asyncio
@patch("datajunction_server.internal.caching.query_cache_manager.get_measures_query")
async def test_fallback_calls_get_measures_query(get_measures_query_mock):
    """
    Should call get_measures_query with correct args.
    """
    get_measures_query_mock.return_value = [{"sql": "SELECT * FROM test"}]

    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    params = QueryRequestParams(
        query_type=QueryBuildType.MEASURES,
        nodes=["foo"],
        dimensions=["dim1"],
        filters=[],
    )
    request = DummyRequest()
    result = await manager.fallback(request, params)

    get_measures_query_mock.assert_called_once()
    assert result == [{"sql": "SELECT * FROM test"}]


@pytest.mark.asyncio
@patch("datajunction_server.internal.caching.query_cache_manager.get_measures_query")
@patch(
    "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
)
async def test_get_or_load_respects_cache_control(
    version_query_request_mock,
    get_measures_query_mock,
):
    """
    Full flow test to ensure Cache-Control is respected.
    """
    version_query_request_mock.return_value = "versioned123"
    get_measures_query_mock.return_value = [{"sql": "SELECT * FROM test"}]

    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    params = QueryRequestParams(
        query_type=QueryBuildType.MEASURES,
        nodes=["foo"],
        dimensions=["dim1"],
        filters=[],
    )

    # Put stale value in cache to test hit vs miss
    key = await manager.build_cache_key(DummyRequest(), params)
    cache.set(key, [{"sql": "CACHED"}])

    background = BackgroundTasks()

    # `no-cache` => should bypass cache
    request = DummyRequest(cache_control="no-cache")
    result = await manager.get_or_load(background, request, params)
    assert result == [{"sql": "SELECT * FROM test"}]

    # Run tasks, should store
    for task in background.tasks:
        await task()
    assert cache.get(key) == [{"sql": "SELECT * FROM test"}]

    # `no-store` => should hit cache, but not store
    cache.set(key, [{"sql": "CACHED"}])
    request = DummyRequest(cache_control="no-store")
    result = await manager.get_or_load(background, request, params)
    assert result == [{"sql": "CACHED"}]  # hits stale

    # `no-cache, no-store` => should always fallback but never store
    request = DummyRequest(cache_control="no-cache, no-store")
    result = await manager.get_or_load(background, request, params)
    assert result == [{"sql": "SELECT * FROM test"}]
    cache.get(key) == [{"sql": "CACHED"}]  # still stale
