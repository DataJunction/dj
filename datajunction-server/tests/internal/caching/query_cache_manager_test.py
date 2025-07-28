from unittest import mock
from unittest.mock import patch

from httpx import AsyncClient
import pytest
from fastapi import BackgroundTasks
from starlette.datastructures import Headers
from sqlalchemy.ext.asyncio import AsyncSession
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
async def test_build_cache_key_calls_versioning():
    """
    Should call version_query_request and build the key.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
        return_value="versioned123",
    ) as version_query_request_mock:
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
async def test_fallback_calls_get_measures_query():
    """
    Should call get_measures_query with correct args.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
        return_value=[{"sql": "SELECT * FROM test"}],
    ) as get_measures_query_mock:
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
async def test_get_or_load_respects_cache_control():
    """
    Full flow test to ensure Cache-Control is respected.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
        return_value=[{"sql": "SELECT * FROM test"}],
    ):
        with patch(
            "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
            return_value="versioned123",
        ):
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


@pytest.mark.asyncio
async def test_measures_get_or_load(
    module__session: AsyncSession,
    module__client_with_roads: AsyncClient,
):
    """
    Test measures SQL get_or_load.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.session_context",
        return_value=module__session,
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
        params = QueryRequestParams(
            query_type=QueryBuildType.MEASURES,
            nodes=["default.avg_repair_price", "default.num_repair_orders"],
            dimensions=["default.dispatcher.company_name"],
            filters=["default.hard_hat.state = 'CA'"],
            engine_name=None,
            engine_version=None,
            limit=None,
            orderby=[],
            other_args=None,
            include_all_columns=False,
            use_materialized=True,
            preaggregate=True,
            query_params="{}",
        )

        # Validate building the cache key
        key = await manager.build_cache_key(DummyRequest(), params)
        assert key.startswith("sql:measures:")

        background = BackgroundTasks()

        # `no-cache` => should bypass cache
        request = DummyRequest(cache_control="no-cache")
        expected_result = await manager.get_or_load(background, request, params)
        assert expected_result == [mock.ANY]

        # Run tasks, should store
        for task in background.tasks:
            await task()
        assert cache.get(key) == expected_result

        # `no-store` => should hit cache, but not store
        cache.set(key, [{"sql": "CACHED"}])
        request = DummyRequest(cache_control="no-store")
        result = await manager.get_or_load(background, request, params)
        assert result == [{"sql": "CACHED"}]  # hits stale

        # `no-cache, no-store` => should always fallback but never store
        request = DummyRequest(cache_control="no-cache, no-store")
        result = await manager.get_or_load(background, request, params)
        assert result == expected_result
        cache.get(key) == [{"sql": "CACHED"}]  # still stale
