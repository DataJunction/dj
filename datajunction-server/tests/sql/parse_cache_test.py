"""Tests for the parsed query cache."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from datajunction_server.internal.deployment.type_inference import parse_query
from datajunction_server.sql.parsing import ast, cache
from datajunction_server.sql.parsing.ast import fast_parse_mode
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.cache import parse_cached


@pytest.fixture(autouse=True)
def empty_cache():
    cache._cache.clear()
    cache._inflight.clear()
    yield
    cache._cache.clear()
    cache._inflight.clear()


def parse_calls(*queries: str) -> int:
    with patch.object(cache, "parse", wraps=parse) as wrapped:
        for query in queries:
            parse_cached(query)
    return wrapped.call_count


def test_repeated_query_is_parsed_once():
    assert parse_calls("SELECT a FROM t", "SELECT a FROM t", "SELECT a FROM t") == 1


def test_parse_query_uses_the_cache():
    query = "SELECT a FROM t"
    first = parse_query(query)
    assert parse_calls(query) == 0
    assert str(first) == str(parse_cached(query))


def test_cached_trees_match_a_fresh_parse():
    query = "SELECT SUM(a) AS total FROM ns.t WHERE b > 1"
    first = parse_cached(query)
    second = parse_cached(query)
    assert str(first) == str(second) == str(parse(query))
    assert second.select.projection[0].parent is second.select


def test_each_caller_gets_its_own_tree():
    query = "SELECT a FROM t"
    parse_cached(query)
    first = parse_cached(query)
    second = parse_cached(query)
    first.select.projection[0] = first.select.projection[0].set_alias(ast.Name("x"))
    assert " a x" in str(first)
    assert str(second) == str(parse(query))
    assert str(parse_cached(query)) == str(parse(query))


def test_fast_mode_trees_are_kept_apart_from_linked_trees():
    query = "SELECT a FROM t"
    with fast_parse_mode():
        fast = parse_cached(query)
        assert parse_calls(query) == 0
    assert fast.select.parent is None
    linked = parse_cached(query)
    assert linked.select.parent is linked
    assert set(cache._cache) == {(query, True), (query, False)}


def test_linked_trees_are_not_returned_in_fast_mode():
    query = "SELECT a FROM t"
    linked = parse_cached(query)
    with fast_parse_mode():
        fast = parse_cached(query)
    assert linked.select.parent is linked
    assert fast.select.parent is None


def test_tree_is_not_kept_when_another_thread_had_fast_mode_on():
    def parse_in_fast_mode(query: str):
        with fast_parse_mode():
            return parse(query)

    with patch.object(cache, "parse", parse_in_fast_mode):
        parse_cached("SELECT a FROM t")
    assert cache._cache == {}


def test_tree_is_not_kept_when_only_its_outer_nodes_are_linked():
    def parse_with_fast_mode_ending_early(query: str):
        with fast_parse_mode():
            tree = parse(query)
        return tree.add_self_as_parent()

    with patch.object(cache, "parse", parse_with_fast_mode_ending_early):
        tree = parse_cached("SELECT a FROM t WHERE b > 1")
    assert tree.select.parent is tree
    assert cache._cache == {}


def test_least_recently_used_query_is_dropped():
    with patch.object(cache, "PARSE_CACHE_SIZE", 2):
        assert parse_calls("SELECT 1 FROM a", "SELECT 2 FROM b") == 2
        assert parse_calls("SELECT 1 FROM a") == 0
        assert parse_calls("SELECT 3 FROM c") == 1
        assert [query for query, _ in cache._cache] == [
            "SELECT 1 FROM a",
            "SELECT 3 FROM c",
        ]
        assert parse_calls("SELECT 2 FROM b") == 1


def test_parse_errors_are_not_cached():
    for _ in range(2):
        with pytest.raises(DJParseException):
            parse_cached("SELECT (")
    assert cache._cache == {}


def run_together(queries: list[str]) -> list:
    """Request each query from its own thread, all starting together."""
    results: list = [None] * len(queries)
    barrier = threading.Barrier(len(queries))

    def work(index: int) -> None:
        barrier.wait()
        try:
            results[index] = parse_cached(queries[index])
        except Exception as exc:  # noqa: BLE001
            results[index] = exc

    threads = [threading.Thread(target=work, args=(i,)) for i in range(len(queries))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def test_concurrent_requests_for_a_new_query_parse_it_once():
    query = "SELECT SUM(a) AS total FROM ns.t WHERE b > 1"
    calls = []

    def slow_parse(text: str):
        calls.append(text)
        time.sleep(0.3)
        return parse(text)

    with patch.object(cache, "parse", slow_parse):
        results = run_together([query] * 8)
    assert calls == [query]
    assert len({id(tree) for tree in results}) == 8
    expected = str(parse(query))
    assert [str(tree) for tree in results] == [expected] * 8
    assert [tree.select.projection[0].parent is tree.select for tree in results] == [
        True,
    ] * 8
    assert cache._inflight == {}


def test_distinct_queries_are_parsed_at_the_same_time():
    queries = [f"SELECT {i} FROM t{i}" for i in range(3)]
    barrier = threading.Barrier(3, timeout=10)

    def parse_when_all_arrive(text: str):
        barrier.wait()
        return parse(text)

    with patch.object(cache, "parse", parse_when_all_arrive):
        with ThreadPoolExecutor(3) as pool:
            trees = list(pool.map(parse_cached, queries))
    assert [str(tree) for tree in trees] == [str(parse(q)) for q in queries]
    assert cache._inflight == {}


def test_failed_parse_clears_the_entry_and_later_calls_work():
    calls = []

    def failing_parse(text: str):
        calls.append(text)
        time.sleep(0.2)
        raise DJParseException("boom")

    with patch.object(cache, "parse", failing_parse):
        results = run_together(["SELECT ("] * 4)
    assert [type(result) for result in results] == [DJParseException] * 4
    assert [str(result) for result in results] == ["boom"] * 4
    # Waiters retry on their own, so there is at least one and at most four parses.
    assert 1 <= len(calls) <= 4
    assert cache._inflight == {}
    assert cache._cache == {}
    assert str(parse_cached("SELECT a FROM t")) == str(parse("SELECT a FROM t"))
    assert cache._inflight == {}


def test_failure_in_one_thread_does_not_stop_waiters_from_parsing():
    query = "SELECT a FROM t"
    first_started = threading.Event()
    release = threading.Event()
    calls = []

    def fail_then_succeed(text: str):
        calls.append(text)
        if len(calls) == 1:
            first_started.set()
            release.wait(10)
            raise DJParseException("first parse failed")
        return parse(text)

    with patch.object(cache, "parse", fail_then_succeed):
        with ThreadPoolExecutor(2) as pool:
            leader = pool.submit(parse_cached, query)
            assert first_started.wait(10)
            waiter = pool.submit(parse_cached, query)
            time.sleep(0.2)
            release.set()
            with pytest.raises(DJParseException, match="first parse failed"):
                leader.result(10)
            assert str(waiter.result(10)) == str(parse(query))
    assert calls == [query, query]
    assert cache._inflight == {}
