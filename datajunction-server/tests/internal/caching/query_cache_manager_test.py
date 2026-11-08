import asyncio
from types import SimpleNamespace
from unittest import mock
from unittest.mock import patch

import pytest
from fastapi import BackgroundTasks
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import FastAPI
from starlette.datastructures import Headers

from datajunction_server.database.queryrequest import QueryBuildType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.access.authorization import AccessChecker
from datajunction_server.internal.caching.cachelib_cache import CachelibCache
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)


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
        self.app = FastAPI()

        # Add state with a dummy user for get_current_user
        self.state = SimpleNamespace(
            user=User(
                username="testuser",
                email="test@example.com",
                oauth_provider=OAuthProvider.BASIC,
            ),
        )


@pytest.mark.asyncio
async def test_cache_key_prefix_uses_query_type():
    """
    The cache key prefix should include the query type.
    """
    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    assert manager.cache_key_prefix == "sql:measures"


def test_measures_metric_tags_query_version_is_v2():
    """
    QueryBuildType.MEASURES routes through get_measures_query (the v2 builder),
    so its emitted query_version tag must be "v2", not "v3". The /sql/measures/v3/
    endpoint emits its own v3 tag directly and does not go through the cache manager.
    """
    cache = CachelibCache()
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
    assert manager._metric_tags["query_version"] == "v2"


def test_metric_tags_query_version_for_all_types():
    """
    Every cache-managed query type should emit query_version=v2, since they all
    route through v2 SQL builders.
    """
    cache = CachelibCache()
    for query_type in (
        QueryBuildType.METRICS,
        QueryBuildType.MEASURES,
        QueryBuildType.NODE,
    ):
        assert (
            QueryCacheManager(cache, query_type)._metric_tags["query_version"] == "v2"
        )


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
    mock_access_checker = mock.AsyncMock(spec=AccessChecker)
    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
            return_value=[{"sql": "SELECT * FROM test"}],
        ) as get_measures_query_mock,
        patch(
            "datajunction_server.internal.caching.query_cache_manager.build_access_checker_from_request",
            return_value=mock_access_checker,
        ),
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
        params = QueryRequestParams(
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
    mock_access_checker = mock.AsyncMock(spec=AccessChecker)
    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
            return_value=[{"sql": "SELECT * FROM test"}],
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
            return_value="versioned123",
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager.build_access_checker_from_request",
            return_value=mock_access_checker,
        ),
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
        params = QueryRequestParams(
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
async def test_build_cache_key(
    module__session: AsyncSession,
    module__client_with_roads: AsyncClient,
):
    """
    Check that the cache key is built correctly with versioning.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.session_context",
        return_value=module__session,
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
        params1 = QueryRequestParams(
            nodes=["default.avg_repair_price", "default.num_repair_orders"],
            dimensions=["default.dispatcher.company_name", "default.hard_hat.state"],
            filters=["default.hard_hat.state = 'CA'", "default.hard_hat.state = 'NY'"],
            engine_name=None,
            engine_version=None,
            limit=1000,
            orderby=[],
            other_args=None,
            include_all_columns=False,
            use_materialized=True,
            preaggregate=True,
            query_params="{}",
        )
        # Shuffled ordering
        params2 = QueryRequestParams(
            nodes=["default.num_repair_orders", "default.avg_repair_price"],
            dimensions=["default.hard_hat.state", "default.dispatcher.company_name"],
            filters=["default.hard_hat.state = 'NY'", "default.hard_hat.state = 'CA'"],
            engine_name=None,
            engine_version=None,
            limit=1000,
            orderby=[],
            other_args=None,
            include_all_columns=False,
            use_materialized=True,
            preaggregate=True,
            query_params="{}",
        )
        request = DummyRequest()
        key1 = await manager.build_cache_key(request, params1)
        key2 = await manager.build_cache_key(request, params2)
        assert key1.startswith("sql:measures:")
        assert key2.startswith("sql:measures:")
        assert key1 == key2


@pytest.mark.asyncio
async def test_measures_get_or_load(
    module__session: AsyncSession,
    module__client_with_roads: AsyncClient,
    module__background_tasks,
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
        for func, f_args, f_kwargs in module__background_tasks:
            result = func(*f_args, **f_kwargs)
            if asyncio.iscoroutine(result):
                await result
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


@pytest.mark.asyncio
async def test_cache_backend_error_falls_back_to_fresh_value():
    """
    When the cache backend raises an exception on get, the manager should
    log the error and fall back to computing a fresh value.
    """
    mock_access_checker = mock.AsyncMock(spec=AccessChecker)
    broken_cache = mock.MagicMock()
    broken_cache.get.side_effect = RuntimeError("cache unavailable")

    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
            return_value=[{"sql": "FRESH"}],
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
            return_value="versioned123",
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager.build_access_checker_from_request",
            return_value=mock_access_checker,
        ),
    ):
        manager = QueryCacheManager(broken_cache, QueryBuildType.MEASURES)
        params = QueryRequestParams(nodes=["foo"], dimensions=[], filters=[])
        background = BackgroundTasks()
        request = DummyRequest()

        result = await manager.get_or_load(background, request, params)

        assert result == [{"sql": "FRESH"}]
        broken_cache.get.assert_called()


@pytest.mark.asyncio
async def test_fallback_uses_the_session_it_is_given():
    """
    A build run by the caller, as with Cache-Control: no-cache, uses the caller's
    session instead of opening another.
    """
    session = mock.AsyncMock(spec=AsyncSession)
    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.get_measures_query",
            return_value=[{"sql": "SELECT * FROM test"}],
        ) as get_measures_query_mock,
        patch(
            "datajunction_server.internal.caching.query_cache_manager.build_access_checker_from_request",
            return_value=mock.AsyncMock(spec=AccessChecker),
        ),
    ):
        manager = QueryCacheManager(CachelibCache(), QueryBuildType.MEASURES)
        params = QueryRequestParams(nodes=["foo"], dimensions=[], filters=[])

        result = await manager.fallback(DummyRequest(), params, session)

        assert result == [{"sql": "SELECT * FROM test"}]
        assert get_measures_query_mock.call_args.kwargs["session"] is session


class FailingMarkerCache(CachelibCache):
    """
    Cache whose refresh markers cannot be read.
    """

    def get(self, key: str):
        if key.startswith("refresh:"):
            raise RuntimeError("marker read failed")
        return super().get(key)


async def _hit(manager, cache, cache_control=None, params=None):
    """
    Make one cache hit and return the refresh tasks it scheduled.
    """
    background = mock.Mock(spec=BackgroundTasks)
    result = await manager.get_or_load(
        background,
        DummyRequest(cache_control=cache_control),
        params or PARAMS,
    )
    return result, [
        call.args[0].__name__ for call in background.add_task.call_args_list
    ]


PARAMS = QueryRequestParams(nodes=["foo"], dimensions=["dim1"], filters=[])


@pytest.fixture
async def hot_manager():
    """
    A manager whose cache already holds the result for PARAMS.
    """
    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
            return_value="versioned123",
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager._pending_refresh_keys",
            set(),
        ),
        patch(
            "datajunction_server.internal.caching.query_cache_manager.settings.query_cache_refresh_interval",
            300,
        ),
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.MEASURES)
        key = await manager.build_cache_key(DummyRequest(), PARAMS)
        cache.set(key, [{"sql": "CACHED"}])
        yield manager, cache, key


@pytest.mark.asyncio
async def test_refresh_once_per_interval(hot_manager):
    """
    A second hit inside the interval schedules no refresh.
    """
    from datajunction_server.internal.caching import query_cache_manager as qcm

    manager, cache, key = hot_manager
    assert await _hit(manager, cache) == (
        [{"sql": "CACHED"}],
        ["_refresh_cache_rate_limited"],
    )
    assert cache.get(f"refresh:{key}") == 1

    qcm._pending_refresh_keys.clear()
    assert await _hit(manager, cache) == ([{"sql": "CACHED"}], [])


@pytest.mark.asyncio
async def test_refresh_after_marker_expires(hot_manager):
    """
    Once the marker is gone, the next hit schedules a refresh.
    """
    from datajunction_server.internal.caching import query_cache_manager as qcm

    manager, cache, key = hot_manager
    await _hit(manager, cache)
    qcm._pending_refresh_keys.clear()
    cache.delete(f"refresh:{key}")
    assert await _hit(manager, cache) == (
        [{"sql": "CACHED"}],
        ["_refresh_cache_rate_limited"],
    )


@pytest.mark.asyncio
async def test_refresh_when_interval_is_zero(hot_manager):
    """
    A zero interval refreshes on every hit.
    """
    from datajunction_server.internal.caching import query_cache_manager as qcm

    manager, cache, key = hot_manager
    with patch.object(qcm.settings, "query_cache_refresh_interval", 0):
        await _hit(manager, cache)
        qcm._pending_refresh_keys.clear()
        assert await _hit(manager, cache) == (
            [{"sql": "CACHED"}],
            ["_refresh_cache_rate_limited"],
        )
    assert cache.get(f"refresh:{key}") is None


@pytest.mark.asyncio
async def test_refresh_when_marker_fails(hot_manager):
    """
    A marker error falls back to refreshing on the hit.
    """
    from datajunction_server.internal.caching import query_cache_manager as qcm

    manager, cache, key = hot_manager
    manager.cache = FailingMarkerCache()
    manager.cache.set(key, [{"sql": "CACHED"}])
    for _ in range(2):
        qcm._pending_refresh_keys.clear()
        assert await _hit(manager, manager.cache) == (
            [{"sql": "CACHED"}],
            ["_refresh_cache_rate_limited"],
        )


@pytest.mark.asyncio
async def test_no_store_hit_skips_marker(hot_manager):
    """
    A no-store hit schedules nothing and leaves no marker.
    """
    manager, cache, key = hot_manager
    assert await _hit(manager, cache, "no-store") == ([{"sql": "CACHED"}], [])
    assert cache.get(f"refresh:{key}") is None


@pytest.mark.asyncio
async def test_no_cache_skips_marker(hot_manager):
    """
    A no-cache request builds fresh and leaves no marker.
    """
    manager, cache, key = hot_manager
    with patch.object(
        QueryCacheManager,
        "fallback",
        return_value=[{"sql": "FRESH"}],
    ):
        result, tasks = await _hit(manager, cache, "no-cache")
    assert (result, tasks) == ([{"sql": "FRESH"}], ["_set_cache_with_error_handling"])
    assert cache.get(f"refresh:{key}") is None


BARE_METRIC = QueryRequestParams(nodes=["foo"], dimensions=[], filters=[])


@pytest.mark.parametrize(
    "query_type, params, expected",
    [
        (QueryBuildType.METRICS, BARE_METRIC, True),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo"], dimensions=[""], filters=[]),
            True,
        ),
        (QueryBuildType.MEASURES, BARE_METRIC, False),
        (QueryBuildType.NODE, BARE_METRIC, False),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo", "bar"], dimensions=[], filters=[]),
            False,
        ),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo"], dimensions=["dim1"], filters=[]),
            False,
        ),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo"], dimensions=[], filters=["a = 1"]),
            False,
        ),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo"], dimensions=[], filters=[], orderby=["x"]),
            False,
        ),
        (
            QueryBuildType.METRICS,
            QueryRequestParams(nodes=["foo"], dimensions=[], filters=[], limit=5),
            False,
        ),
    ],
)
def test_is_single_metric(query_type, params, expected):
    """
    Only a metrics request for one bare metric counts.
    """
    assert (
        QueryCacheManager(CachelibCache(), query_type)._is_single_metric(
            params,
        )
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params, all_ancestors",
    [(BARE_METRIC, True), (PARAMS, False)],
)
async def test_key_versions_all_ancestors_of_one_metric(params, all_ancestors):
    """
    Only a bare metric's key is versioned by everything upstream.
    """
    with patch(
        "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
        return_value="versioned123",
    ) as version:
        manager = QueryCacheManager(CachelibCache(), QueryBuildType.METRICS)
        await manager.build_cache_key(DummyRequest(), params)
    assert version.call_args.kwargs["all_ancestors"] is all_ancestors


@pytest.mark.asyncio
async def test_bare_metric_uses_its_own_refresh_interval():
    """
    A bare metric refreshes on its own interval, other requests on theirs.
    """
    from datajunction_server.internal.caching import query_cache_manager as qcm

    with (
        patch(
            "datajunction_server.internal.caching.query_cache_manager.VersionedQueryKey.version_query_request",
            return_value="versioned123",
        ),
        patch.object(qcm, "_pending_refresh_keys", set()),
        patch.object(qcm.settings, "query_cache_refresh_interval", 0),
        patch.object(qcm.settings, "query_cache_single_metric_refresh_interval", 300),
    ):
        cache = CachelibCache()
        manager = QueryCacheManager(cache, QueryBuildType.METRICS)
        for params in (BARE_METRIC, PARAMS):
            cache.set(await manager.build_cache_key(DummyRequest(), params), [1])

        outcomes = {}
        for name, params in (("bare", BARE_METRIC), ("other", PARAMS)):
            tasks = []
            for _ in range(2):
                qcm._pending_refresh_keys.clear()
                tasks.append((await _hit(manager, cache, params=params))[1])
            outcomes[name] = tasks
    assert outcomes == {
        "bare": [["_refresh_cache_rate_limited"], []],
        "other": [["_refresh_cache_rate_limited"], ["_refresh_cache_rate_limited"]],
    }
