"""
Tests for custom antlr4 parser
"""
# mypy: ignore-errors

import pytest

from datajunction_server.sql.parsing.backends.antlr4 import parse


@pytest.mark.parametrize(
    "query_string",
    [
        """SELECT suit, key, value
    FROM suites_and_ranks_arrays
    LATERAL VIEW EXPLODE(rankmap) AS key, value
    ORDER BY suit;""",
        """SELECT suit, exploded_rank
    FROM suites_and_ranks_arrays
    LATERAL VIEW EXPLODE(rank) exploded_rank
    ORDER BY suit;""",
        """SELECT suit, exploded_rank, key, value
    FROM suites_and_ranks_arrays
    LATERAL VIEW EXPLODE(rank) AS exploded_rank
    LATERAL VIEW EXPLODE(rankmap) AS key, value
    ORDER BY suit;""",
    ],
)
def test_antlr4_backend_lateral_view_explode(query_string):
    """
    Test LATERAL VIEW EXPLODE queries
    """
    parse(query_string)


@pytest.mark.parametrize(
    "query_string",
    [
        """Select suit, exploded_rank, exploded_rank2
    from suites_and_ranks_arrays
    CROSS JOIN UNNEST(rank) as t(exploded_rank)
    ORDER BY suit;""",
        """Select suit, exploded_rank, exploded_rank2
    from suites_and_ranks_arrays
    CROSS JOIN UNNEST(rank, rank) as t(exploded_rank, exploded_rank2)
    ORDER BY suit;""",
        """Select suit, exploded_rank, exploded_rank2
    from suites_and_ranks_arrays
    CROSS JOIN UNNEST(rank) as t(exploded_rank)
    CROSS JOIN UNNEST(rank) as t(exploded_rank2)""",
        """Select suit, key, value
    from suites_and_ranks_arrays
    CROSS JOIN UNNEST(rankmap) as t(key, value)
    ORDER BY suit;""",
        """Select suit, exploded_rank, k, value
    from suites_and_ranks_arrays
    CROSS JOIN UNNEST(rank, rankmap) as t(exploded_rank, key, value)
    ORDER BY suit;""",
    ],
)
def test_antlr4_backend_cross_join_unnest(query_string):
    """
    Test CROSS JOIN UNNEST queries
    """
    parse(query_string)


def test_antlr4_backend_predicate_like():
    """
    Test LIKE predicate
    """
    query = parse("SELECT * FROM person WHERE name LIKE '%$_%';")
    assert "LIKE '%$_%'" in str(query)


def test_antlr4_backend_predicate_ilike():
    """
    Test ILIKE predicate
    """
    query = parse("SELECT * FROM person WHERE name ILIKE '%foo%';")
    assert "ILIKE '%foo%'" in str(query)


def test_antlr4_backend_predicate_rlike():
    """
    Test RLIKE predicate
    """
    query = parse("SELECT * FROM person WHERE name RLIKE 'M+';")
    assert "RLIKE 'M+'" in str(query)


def test_antlr4_backend_predicate_is_distinct_from():
    """
    Test IS DISTINCT FROM predicate
    """
    query = parse("SELECT * FROM person WHERE name IS DISTINCT FROM 'Bob'")
    assert "IS DISTINCT FROM 'Bob'" in str(query)


def test_antlr4_backend_trim():
    """
    Test trim
    """
    query = parse("SELECT TRIM(BOTH FROM '    SparkSQL   ');")
    assert "TRIM( BOTH FROM  '    SparkSQL   ')" in str(query)
    query = parse("SELECT TRIM(LEADING FROM '    SparkSQL   ');")
    assert "TRIM( LEADING FROM  '    SparkSQL   ')" in str(query)
    query = parse("SELECT TRIM(TRAILING FROM '    SparkSQL   ');")
    assert "TRIM( TRAILING FROM  '    SparkSQL   ')" in str(query)
    query = parse("SELECT TRIM('    SparkSQL   ');")
    assert "TRIM('    SparkSQL   ')" in str(query)


def test_antlr4_lambda_function():
    """
    Test a lambda function using `->`
    """
    query = parse("SELECT FOO('a', 'b', c -> d) AS e;")
    assert "FOO('a', 'b', c -> d) AS e" in str(query)
    query = parse("SELECT FOO('a', 'b', (c, c2, c3) -> d) AS e;")
    assert "FOO('a', 'b', (c, c2, c3) -> d) AS e" in str(query)
