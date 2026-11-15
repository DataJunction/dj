"""Tests for the parsed query cache."""

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
