"""Tests for parsing queries in worker processes."""

import pickle
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from unittest.mock import patch

import pytest

from datajunction_server.sql.parsing import ast, cache, process_pool
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.process_pool import (
    _decode,
    _parse_batch,
    get_pool,
    parse_in_pool,
    shutdown_pool,
)

QUERIES = {
    "ns.a": "SELECT a FROM t",
    "ns.b": "SELECT SUM(x) AS total FROM ns.t WHERE y > 1 GROUP BY z",
    "ns.bad": "SELEC FROM WHERE",
    "ns.c": "SELECT 1",
}


@pytest.fixture(autouse=True)
def clean_state():
    cache._cache.clear()
    shutdown_pool()
    yield
    cache._cache.clear()
    shutdown_pool()


def describe(results: dict) -> dict:
    """Trees as their SQL, errors as their type and message."""
    return {
        name: (type(value), str(value)) if isinstance(value, Exception) else str(value)
        for name, value in results.items()
    }


def fresh_parse(queries: dict[str, str]) -> dict:
    results: dict = {}
    for name, query in queries.items():
        try:
            results[name] = parse(query)
        except Exception as exc:
            results[name] = exc
    return results


@pytest.fixture
def thread_pool():
    """Stand-in for the worker processes that runs in this process."""
    with ThreadPoolExecutor(max_workers=2) as pool:
        with patch.object(process_pool, "get_pool", return_value=pool):
            yield pool


@pytest.mark.asyncio
async def test_results_match_a_fresh_parse_in_real_workers():
    results = await parse_in_pool(QUERIES, 2)
    assert describe(results) == describe(fresh_parse(QUERIES))
    assert isinstance(results["ns.bad"], Exception)
    assert set(cache._cache) == {(QUERIES[name], False) for name in QUERIES} - {
        (QUERIES["ns.bad"], False),
    }
    assert isinstance(process_pool._pool, ProcessPoolExecutor)
    workers = list(process_pool._pool._processes.values())
    shutdown_pool()
    for worker in workers:
        worker.join(10)
    assert [worker.is_alive() for worker in workers] == [False] * len(workers)


@pytest.mark.asyncio
async def test_cached_queries_are_not_sent_to_the_pool(thread_pool):
    cache.store("SELECT a FROM t", cache.dump(parse("SELECT a FROM t"), False), False)
    queries = {"ns.a": "SELECT a FROM t", "ns.c": "SELECT 1"}
    with patch.object(process_pool, "_parse_batch", wraps=_parse_batch) as batch:
        results = await parse_in_pool(queries, 2)
    assert batch.call_args_list == [((["SELECT 1"], False),)]
    assert describe(results) == describe(fresh_parse(queries))


@pytest.mark.asyncio
async def test_nothing_is_sent_when_everything_is_cached():
    for query in QUERIES.values():
        try:
            cache.parse_cached(query)
        except Exception:
            pass
    queries = {name: q for name, q in QUERIES.items() if name != "ns.bad"}
    with patch.object(process_pool, "get_pool", side_effect=AssertionError):
        results = await parse_in_pool(queries, 2)
    assert describe(results) == describe(fresh_parse(queries))


@pytest.mark.asyncio
async def test_results_are_stored_in_the_cache(thread_pool):
    await parse_in_pool({"ns.a": "SELECT a FROM t"}, 2)
    stored = cache.lookup("SELECT a FROM t")
    assert str(stored) == str(parse("SELECT a FROM t"))
    assert stored.select.projection[0].parent is stored.select


@pytest.mark.asyncio
async def test_queries_are_sent_in_batches(thread_pool):
    queries = {f"ns.q{i}": f"SELECT {i}" for i in range(10)}
    with patch.object(process_pool, "_parse_batch", wraps=_parse_batch) as batch:
        results = await parse_in_pool(queries, 1)
    assert sorted(len(call.args[0]) for call in batch.call_args_list) == [1, 3, 3, 3]
    assert describe(results) == describe(fresh_parse(queries))
    assert list(results) == list(queries)


@pytest.mark.asyncio
async def test_fast_mode_is_passed_to_the_workers(thread_pool):
    with ast.fast_parse_mode():
        results = await parse_in_pool({"ns.a": "SELECT a FROM t"}, 2)
        assert results["ns.a"].select.parent is None
    assert set(cache._cache) == {("SELECT a FROM t", True)}
    ast._fast_parse = False


@pytest.mark.asyncio
async def test_a_broken_pool_raises_and_is_replaced():
    await parse_in_pool({"ns.c": "SELECT 1"}, 1)
    first = process_pool._pool
    for worker in list(first._processes.values()):
        worker.kill()
    time.sleep(1)
    with pytest.raises(BrokenProcessPool):
        await parse_in_pool({"ns.a": "SELECT a FROM t"}, 1)
    assert process_pool._pool is None
    results = await parse_in_pool({"ns.a": "SELECT a FROM t"}, 1)
    assert describe(results) == describe(fresh_parse({"ns.a": "SELECT a FROM t"}))
    assert process_pool._pool is not first


def test_pool_is_reused_and_resized():
    first = get_pool(1)
    assert get_pool(1) is first
    second = get_pool(2)
    assert second is not first
    shutdown_pool()
    assert process_pool._pool is None
    shutdown_pool()


def test_worker_returns_trees_and_errors_as_values():
    outcomes = _parse_batch(["SELECT a FROM t", "SELEC FROM"], False)
    tree_blob, cacheable = outcomes[0]
    assert (pickle.loads(tree_blob).__class__, cacheable) == (ast.Query, True)
    assert outcomes[1][0] is None
    assert type(pickle.loads(outcomes[1][1])) is type(
        fresh_parse({"x": "SELEC FROM"})["x"],
    )


def test_worker_marks_unlinked_trees_as_not_cacheable():
    with patch.object(process_pool.cache, "dump", return_value=None):
        [(blob, cacheable)] = _parse_batch(["SELECT a FROM t"], False)
    assert (str(pickle.loads(blob)), cacheable) == (
        str(parse("SELECT a FROM t")),
        False,
    )


def test_worker_handles_errors_that_cannot_be_pickled():
    class Odd(Exception):
        def __init__(self):
            super().__init__("odd failure")
            self.local = lambda: None

    with patch(
        "datajunction_server.sql.parsing.backends.antlr4.parse",
        side_effect=Odd(),
    ):
        outcomes = _parse_batch(["SELECT 1"], False)
    assert outcomes == [(None, None, "odd failure")]


def test_decode_cases():
    tree = parse("SELECT 1")
    blob = pickle.dumps(tree)
    assert str(_decode("SELECT 1", (blob, False), False)) == str(tree)
    assert cache._cache == {}
    assert str(_decode("SELECT 1", (blob, True), False)) == str(tree)
    assert set(cache._cache) == {("SELECT 1", False)}
    error = _decode("q", (None, pickle.dumps(ValueError("bad")), "bad"), False)
    assert (type(error), str(error)) == (ValueError, "bad")
    for pickled in (None, b"junk"):
        error = _decode("q", (None, pickled, "bad"), False)
        assert (type(error), str(error)) == (RuntimeError, "bad")


def test_warm_up_parses_its_queries():
    with patch(
        "datajunction_server.sql.parsing.backends.antlr4.parse",
    ) as parse_mock:
        process_pool._warm_up()
    assert [c.args[0] for c in parse_mock.call_args_list] == list(
        process_pool.WARMUP_QUERIES,
    )
