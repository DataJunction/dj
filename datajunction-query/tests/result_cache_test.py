from unittest.mock import patch

from cachelib.simple import SimpleCache

from djqs.models.query import QueryCreate, QueryResults
from djqs.result_cache import (
    CachedQueryResult,
    DEFAULT_FRESHNESS_SECONDS,
    MAX_RETENTION_SECONDS,
    build_result_cache_key,
    get_cached_result,
    has_stale_while_revalidate,
    parse_cache_control,
)


def test_cache_control_uses_positive_max_age_for_retention():
    assert parse_cache_control("max-age=172800") == (False, False, 172800)
    assert parse_cache_control("max-age=999999999") == (
        False,
        False,
        MAX_RETENTION_SECONDS,
    )
    assert parse_cache_control("no-cache, no-store, max-age=0") == (
        True,
        True,
        24 * 60 * 60,
    )
    assert parse_cache_control("max-age=invalid") == (False, False, 24 * 60 * 60)
    assert has_stale_while_revalidate("max-age=60, stale-while-revalidate") is True
    assert has_stale_while_revalidate("stale-while-revalidate=60") is True
    assert has_stale_while_revalidate("max-age=60") is False


def test_result_cache_key_ignores_async_execution_mode():
    first = QueryCreate(
        catalog_name="warehouse",
        engine_name="trino",
        engine_version="1",
        submitted_query="SELECT 1",
        async_=True,
    )
    second = QueryCreate(
        catalog_name="warehouse",
        engine_name="trino",
        engine_version="1",
        submitted_query="SELECT 1",
        async_=False,
    )
    assert build_result_cache_key(first) == build_result_cache_key(second)
    assert build_result_cache_key(first, {"SQLALCHEMY_URI": "postgres://one"}) != (
        build_result_cache_key(first, {"SQLALCHEMY_URI": "postgres://two"})
    )


def test_get_cached_result_distinguishes_fresh_and_stale_entries():
    cache = SimpleCache()
    key = "result"
    result = QueryResults(submitted_query="SELECT 1")

    with patch("djqs.result_cache.time.time", return_value=100):
        cache.set(key, CachedQueryResult(result=result, created_at=100))
        cached, is_fresh = get_cached_result(cache, key)
    assert cached == result
    assert is_fresh is True

    with patch(
        "djqs.result_cache.time.time",
        return_value=100 + DEFAULT_FRESHNESS_SECONDS,
    ):
        cached, is_fresh = get_cached_result(cache, key)
    assert cached == result
    assert is_fresh is False
