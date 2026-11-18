"""Tests for the parsed query cache."""

import asyncio
import threading
from unittest.mock import patch

import pytest

from datajunction_server.internal.deployment.type_inference import parse_query
from datajunction_server.sql.parsing import ast, cache
from datajunction_server.sql.parsing.ast import fast_parse_mode
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.cache import parse_cached


@pytest.fixture(autouse=True)
def empty_cache():
    cache._cache.clear()
    yield
    cache._cache.clear()


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


def test_fast_mode_does_not_leak_across_threads():
    query = "SELECT a FROM t"
    inside, outside = threading.Event(), threading.Event()
    result: dict = {}

    def fast_worker():
        with fast_parse_mode():
            inside.set()
            outside.wait()

    worker = threading.Thread(target=fast_worker)
    worker.start()
    inside.wait()
    result["tree"] = parse(query)
    result["cached"] = parse_cached(query)
    outside.set()
    worker.join()
    assert result["tree"].select.parent is result["tree"]
    assert result["cached"].select.parent is result["cached"]
    assert list(cache._cache) == [(query, False)]


def test_fast_mode_nesting_restores_previous_value():
    assert ast._fast_parse.get() is False
    with fast_parse_mode():
        with fast_parse_mode():
            assert ast._fast_parse.get() is True
        assert ast._fast_parse.get() is True
    assert ast._fast_parse.get() is False


def test_fast_mode_does_not_leak_into_to_thread():
    async def run():
        def read():
            return ast._fast_parse.get()

        with fast_parse_mode():
            inherited = await asyncio.to_thread(read)

            def toggle():
                with fast_parse_mode():
                    pass

            await asyncio.to_thread(toggle)
            return inherited, ast._fast_parse.get()

    assert asyncio.run(run()) == (True, True)
    assert ast._fast_parse.get() is False
