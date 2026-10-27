"""
Requests that miss on the same key share one build, and across processes the
one holding the cache's lock builds while the others wait for the result.
"""

import asyncio
from unittest.mock import patch

import pytest

from datajunction_server.database.queryrequest import QueryBuildType
from datajunction_server.internal.caching import query_cache_manager as qcm
from datajunction_server.internal.caching.interface import CacheInterface
from datajunction_server.internal.caching.query_cache_manager import (
    QueryCacheManager,
    QueryRequestParams,
)

KEY = "sql:measures:abc"
PARAMS = QueryRequestParams(nodes=["foo"], dimensions=[], filters=[])


class LockingCache(CacheInterface):
    """An in-memory cache with locks, standing in for one shared by processes."""

    def __init__(self):
        self.values: dict = {}
        self.locks: set[str] = set()
        self.lock_calls = 0

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, timeout=300):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

    def try_lock(self, key, lease):
        self.lock_calls += 1
        if key in self.locks:
            return False
        self.locks.add(key)
        return True

    def unlock(self, key):
        self.locks.discard(key)


@pytest.fixture(autouse=True)
def quick_waits():
    with (
        patch.object(qcm, "_LOCK_POLL_SECONDS", 0.02),
        patch.object(qcm.settings, "query_cache_lock_wait", 0.3),
    ):
        yield


def manager_building(cache, builds: list, result=None, delay=0.1):
    manager = QueryCacheManager(cache, QueryBuildType.MEASURES)

    async def fallback(request, params, session=None):
        builds.append(params)
        await asyncio.sleep(delay)
        if isinstance(result, Exception):
            raise result
        return result if result is not None else [{"sql": "BUILT"}]

    manager.fallback = fallback  # type: ignore[method-assign]
    return manager


async def test_concurrent_misses_on_one_process_build_once():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds)

    results = await asyncio.gather(
        *[manager._coalesced_build(KEY, None, PARAMS, None) for _ in range(20)],  # type: ignore[arg-type]
    )

    assert len(builds) == 1
    assert all(result == [{"sql": "BUILT"}] for result in results)
    assert cache.values[KEY] == [{"sql": "BUILT"}]
    assert cache.lock_calls == 1


async def test_lock_is_released_after_the_build():
    cache = LockingCache()
    manager = manager_building(cache, [])

    await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert cache.locks == set()


async def test_lock_is_released_when_the_build_fails():
    cache = LockingCache()
    manager = manager_building(cache, [], result=ValueError("bad query"))

    with pytest.raises(ValueError, match="bad query"):
        await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert cache.locks == set()
    assert qcm._inflight_builds == {}


async def test_failed_build_fails_every_waiter_on_the_process():
    manager = manager_building(LockingCache(), [], result=ValueError("bad query"))

    results = await asyncio.gather(
        *[manager._coalesced_build(KEY, None, PARAMS, None) for _ in range(3)],  # type: ignore[arg-type]
        return_exceptions=True,
    )

    assert [type(result) for result in results] == [ValueError] * 3


async def test_waits_for_the_process_holding_the_lock():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds)
    cache.try_lock(KEY, 60)  # another process is building it

    async def other_process_finishes():
        await asyncio.sleep(0.1)
        cache.set(KEY, [{"sql": "FROM OTHER PROCESS"}])
        cache.unlock(KEY)

    other = asyncio.create_task(other_process_finishes())
    result = await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]
    await other

    assert result == [{"sql": "FROM OTHER PROCESS"}]
    assert builds == []


async def test_builds_anyway_when_the_holder_never_delivers():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds)
    cache.try_lock(KEY, 60)  # held, and never released

    result = await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert result == [{"sql": "BUILT"}]
    assert len(builds) == 1
    assert KEY in cache.locks  # the other process's lock is not ours to release


async def test_uses_what_the_last_holder_stored_before_we_got_the_lock():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds)
    original_try_lock = cache.try_lock

    def stored_just_before_the_lock(key, lease):
        cache.set(key, [{"sql": "JUST STORED"}])
        return original_try_lock(key, lease)

    cache.try_lock = stored_just_before_the_lock  # type: ignore[method-assign]

    result = await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert result == [{"sql": "JUST STORED"}]
    assert builds == []


async def test_builds_when_the_lock_backend_fails():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds)

    def broken(*_args):
        raise RuntimeError("kv unavailable")

    cache.try_lock = broken  # type: ignore[method-assign]
    cache.unlock = broken  # type: ignore[method-assign]

    result = await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert result == [{"sql": "BUILT"}]
    assert len(builds) == 1


async def test_failing_to_release_the_lock_does_not_fail_the_request():
    cache = LockingCache()
    manager = manager_building(cache, [])

    def broken(_key):
        raise RuntimeError("kv unavailable")

    cache.unlock = broken  # type: ignore[method-assign]

    result = await manager._coalesced_build(KEY, None, PARAMS, None)  # type: ignore[arg-type]

    assert result == [{"sql": "BUILT"}]


async def test_the_request_that_started_the_build_going_away_does_not_fail_others():
    cache, builds = LockingCache(), []
    manager = manager_building(cache, builds, delay=0.2)

    first = asyncio.create_task(manager._coalesced_build(KEY, None, PARAMS, None))  # type: ignore[arg-type]
    await asyncio.sleep(0.05)
    second = asyncio.create_task(manager._coalesced_build(KEY, None, PARAMS, None))  # type: ignore[arg-type]
    await asyncio.sleep(0.05)
    first.cancel()

    assert await second == [{"sql": "BUILT"}]
    assert len(builds) == 1
    assert cache.values[KEY] == [{"sql": "BUILT"}]


async def test_a_cache_without_locks_still_builds_once_per_process():
    class NoLocks(CacheInterface):
        def __init__(self):
            self.values = {}

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, timeout=300):
            self.values[key] = value

        def delete(self, key):
            self.values.pop(key, None)

    cache, builds = NoLocks(), []
    manager = manager_building(cache, builds)

    await asyncio.gather(
        *[manager._coalesced_build(KEY, None, PARAMS, None) for _ in range(5)],  # type: ignore[arg-type]
    )

    assert len(builds) == 1
    assert cache.values[KEY] == [{"sql": "BUILT"}]
