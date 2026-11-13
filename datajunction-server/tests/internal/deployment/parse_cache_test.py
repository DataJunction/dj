"""Tests for the parse cache behind `parse_query`."""

from unittest.mock import patch

import pytest

from datajunction_server.internal.deployment import type_inference
from datajunction_server.internal.deployment.type_inference import parse_query
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import fast_parse_mode
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


@pytest.fixture(autouse=True)
def empty_cache():
    type_inference._parse_cache.clear()
    yield
    type_inference._parse_cache.clear()


def parse_calls(*queries: str) -> int:
    with patch.object(type_inference, "parse", wraps=parse) as wrapped:
        for query in queries:
            parse_query(query)
    return wrapped.call_count


def test_repeated_query_is_parsed_once():
    assert parse_calls("SELECT a FROM t", "SELECT a FROM t", "SELECT a FROM t") == 1


def test_cached_trees_match_a_fresh_parse():
    query = "SELECT SUM(a) AS total FROM ns.t WHERE b > 1"
    first = parse_query(query)
    second = parse_query(query)
    assert str(first) == str(second) == str(parse(query))
    assert second.select.projection[0].parent is second.select


def test_each_caller_gets_its_own_tree():
    query = "SELECT a FROM t"
    parse_query(query)
    first = parse_query(query)
    second = parse_query(query)
    first.select.projection[0] = first.select.projection[0].set_alias(ast.Name("x"))
    assert " a x" in str(first)
    assert str(second) == str(parse(query))
    assert str(parse_query(query)) == str(parse(query))


def test_trees_built_in_fast_mode_are_not_kept():
    with fast_parse_mode():
        assert parse_calls("SELECT a FROM t", "SELECT a FROM t") == 2
    assert type_inference._parse_cache == {}


def test_tree_is_not_kept_when_another_thread_had_fast_mode_on():
    def parse_in_fast_mode(query: str):
        with fast_parse_mode():
            return parse(query)

    with patch.object(type_inference, "parse", parse_in_fast_mode):
        parse_query("SELECT a FROM t")
    assert type_inference._parse_cache == {}


def test_least_recently_used_query_is_dropped():
    with patch.object(type_inference, "PARSE_CACHE_SIZE", 2):
        assert parse_calls("SELECT 1 FROM a", "SELECT 2 FROM b") == 2
        assert parse_calls("SELECT 1 FROM a") == 0
        assert parse_calls("SELECT 3 FROM c") == 1
        assert list(type_inference._parse_cache) == [
            "SELECT 1 FROM a",
            "SELECT 3 FROM c",
        ]
        assert parse_calls("SELECT 2 FROM b") == 1


def test_parse_errors_are_not_cached():
    for _ in range(2):
        with pytest.raises(DJParseException):
            parse_query("SELECT (")
    assert type_inference._parse_cache == {}
