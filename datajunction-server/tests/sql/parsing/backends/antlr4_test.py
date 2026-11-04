"""
Tests for custom antlr4 parser
"""
# mypy: ignore-errors

from types import SimpleNamespace

import pytest
from antlr4 import InputStream
from antlr4.tree.Trees import Trees

from datajunction_server.sql.parsing.backends.antlr4 import (
    UpperCaseInputStream,
    _definition_tree_parser,
    _request_tree_parser,
    ast,
    build_parser,
    build_string_parser,
    cached_request_tree,
    parse,
    report_parse_cache_stats,
)
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


def test_antlr4_backend_preserves_identifier_quotes():
    """Quoted and non-reserved identifiers retain their original spelling."""
    query = parse("SELECT `select`, interval FROM `group`")

    identifiers = {(name.name, name.quote_style) for name in query.find_all(ast.Name)}
    assert ("select", "`") in identifiers
    assert ("interval", "") in identifiers
    assert ("group", "`") in identifiers


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
    assert "FOO('a', 'b', c->d) AS e" in str(query)
    query = parse("SELECT FOO('a', 'b', (c, c2, c3) -> d) AS e;")
    assert "FOO('a', 'b', (c, c2, c3)->d) AS e" in str(query)


def test_antlr4_parse_error():
    """
    Test LATERAL VIEW EXPLODE queries
    """
    with pytest.raises(DJParseException):
        parse("SELECT ** FROM 1_#**")


def test_query_parameters():
    """
    Test query parameters
    """
    query = parse("SELECT * FROM person WHERE name = :`param.name`")
    assert ":`param.name`" in str(query)
    assert [param for param in query.find_all(ast.QueryParameter)] == [
        ast.QueryParameter(
            prefix=":",
            name="param.name",
            quote_style="`",
        ),
    ]

    query = parse('SELECT * FROM person WHERE some_map[:"param.name"] IS NOT NULL')
    assert 'some_map[:"param.name"]' in str(query)
    assert [param for param in query.find_all(ast.QueryParameter)] == [
        ast.QueryParameter(
            prefix=":",
            name="param.name",
            quote_style='"',
        ),
    ]

    query = parse("SELECT * FROM person WHERE some_map[:param_name] IS NOT NULL")
    assert "some_map[:param_name]" in str(query)
    assert [param for param in query.find_all(ast.QueryParameter)] == [
        ast.QueryParameter(
            prefix=":",
            name="param_name",
            quote_style="",
        ),
    ]

    query = parse(
        "SELECT * FROM person WHERE some_map[CAST(:param_name AS INT)] IS NOT NULL",
    )
    assert "some_map[CAST(:param_name AS INT)]" in str(query)
    assert [param for param in query.find_all(ast.QueryParameter)] == [
        ast.QueryParameter(
            prefix=":",
            name="param_name",
            quote_style="",
        ),
    ]


def test_antlr4_arithmetic_unary_op():
    """
    Test parsing arithmetic unary operations
    """
    query_ast = parse("SELECT -a")
    assert query_ast.select.projection[0] == ast.ArithmeticUnaryOp(
        op=ast.ArithmeticUnaryOpKind.Minus,
        expr=ast.Column(name=ast.Name(name="a")),
    )
    assert "-a" in str(query_ast)

    query_ast = parse("SELECT +a")
    assert query_ast.select.projection[0] == ast.ArithmeticUnaryOp(
        op=ast.ArithmeticUnaryOpKind.Plus,
        expr=ast.Column(name=ast.Name(name="a")),
    )
    assert "+a" in str(query_ast)

    query_ast = parse("SELECT ~a")
    assert query_ast.select.projection[0] == ast.ArithmeticUnaryOp(
        op=ast.ArithmeticUnaryOpKind.BitwiseNot,
        expr=ast.Column(name=ast.Name(name="a")),
    )
    assert "~a" in str(query_ast)


def test_antlr4_decimal_at_eof():
    """
    Test parsing expressions with decimal numbers at end of input.

    This tests a fix for a bug where the lexer's isValidDecimal() method
    would crash with `chr(-1)` ValueError when a decimal like `3600.0`
    appeared at the end of the input string (EOF).
    """
    # Simple decimal at EOF
    query_ast = parse("SELECT 3600.0")
    assert "3600.0" in str(query_ast)

    # Expression with decimal at EOF
    query_ast = parse("SELECT x / 3600.0")
    assert "3600.0" in str(query_ast)
    assert "/" in str(query_ast)

    # Multiple decimals, last one at EOF
    query_ast = parse("SELECT 1.5 + 2.5")
    assert "1.5" in str(query_ast)
    assert "2.5" in str(query_ast)

    # Decimal with scientific notation at EOF (normalized to 150.0)
    query_ast = parse("SELECT 1.5E2")
    assert "150.0" in str(query_ast)


def test_parse_dangling_join_raises_djparse_not_attribute_error():
    """Malformed SQL where ANTLR recovers without surfacing a syntax error
    (e.g. a dangling ``LEFT JOIN`` with no table) used to crash the visitor
    with ``AttributeError: 'NoneType' object has no attribute 'start'`` and
    log as a server error. It must now surface as ``DJParseException`` so
    the request returns 4xx instead.
    """
    bad_sql = """
    SELECT a.x
    FROM foo a
      LEFT JOIN bar b ON a.id = b.id
      LEFT JOIN
      LEFT JOIN baz c ON a.id = c.id
    """
    with pytest.raises(DJParseException):
        parse(bad_sql)


def test_aliased_relation_single_primary():
    """`JOIN (t) alias` — parenthesized single relation with alias — must parse
    and treat the inner table as if aliased directly. Previously raised
    ``TypeError: No visitor registered for type AliasedRelationContext``."""
    query_ast = parse(
        "SELECT d.col FROM foo CROSS JOIN (bar) d",
    )
    rendered = str(query_ast)
    assert "bar" in rendered
    assert " d" in rendered or "AS d" in rendered


def test_aliased_relation_join_group_rejected():
    """`(a JOIN b) AS x` — aliasing a parenthesized join group — isn't yet
    representable in the AST and must surface as a ``DJParseException`` rather
    than crashing."""
    with pytest.raises(DJParseException, match="join group"):
        parse(
            "SELECT * FROM (a CROSS JOIN b) AS x",
        )


def test_unsupported_grammar_branch_surfaces_djparse():
    """An unknown/unimplemented grammar branch must raise ``DJParseException``,
    not ``TypeError`` — otherwise the validator's parse-error catch misses it
    and the request returns 500."""
    # An empty grouping `()` in a relation position triggers an unhandled
    # context. Whatever the exact shape, a parser visitor gap must be a parse
    # error from the user's perspective.
    bad_sql = "SELECT * FROM foo CROSS JOIN ()"
    with pytest.raises(DJParseException):
        parse(bad_sql)


def test_request_sql_tree_is_cached_between_parses():
    """The same request filter reuses one ANTLR tree instead of re-parsing."""
    _request_tree_parser().cache_clear()
    sql = "SELECT 1 WHERE colx = 'cached'"

    first = cached_request_tree(sql, "singleStatement")
    second = cached_request_tree(sql, "singleStatement")

    assert first is second
    assert _request_tree_parser().cache_info().hits == 1


def test_node_definitions_use_their_own_cache():
    """Node SQL and request SQL are cached separately."""
    _definition_tree_parser().cache_clear()
    _request_tree_parser().cache_clear()

    parse("SELECT defn_col FROM defn_tbl")
    parse("SELECT 1 WHERE colx = 'a'", from_request=True)

    assert _definition_tree_parser().cache_info().currsize == 1
    assert _request_tree_parser().cache_info().currsize == 1


def test_cached_request_tree_yields_independent_asts():
    """Each parse gets a fresh AST, so mutating one cannot corrupt the next."""
    sql = "SELECT 1 WHERE amount = 5"

    first = parse(sql, from_request=True)
    first.select.where.right.value = 99

    second = parse(sql, from_request=True)
    assert str(second) == str(parse(sql, from_request=True))
    assert "99" not in str(second)


def test_report_parse_cache_stats_emits_gauges(mocker):
    """The request cache reports hits, misses and size."""
    provider = mocker.MagicMock()
    mocker.patch(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        return_value=provider,
    )

    report_parse_cache_stats()

    reported = {
        (call.args[0], call.args[2]["cache"]) for call in provider.gauge.call_args_list
    }
    assert reported == {
        (name, cache)
        for name in (
            "dj.sql.parse_cache.hits",
            "dj.sql.parse_cache.misses",
            "dj.sql.parse_cache.size",
            "dj.sql.parse_cache.max_size",
        )
        for cache in ("definitions", "requests")
    }


def test_request_parse_cache_size_comes_from_settings(settings):
    """The cache is sized from settings, not a hardcoded constant."""
    _request_tree_parser.cache_clear()
    assert (
        _request_tree_parser().cache_info().maxsize == settings.request_parse_cache_size
    )


def _tree_string(parser, rule="singleStatement"):
    tree = getattr(parser, rule)()
    return Trees.toStringTree(tree, None, parser)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT a, COUNT(*) AS n FROM t WHERE b > 1 GROUP BY a",
        "select Foo, bar from `Mixed Case`.Table_X where Name like 'AbC%'",
        "SeLeCt CAST(x AS bigint), CaSe WhEn y THEN 1 eLsE 2 EnD FROM t",
        "WITH c AS (SELECT 1 AS one) SELECT * FROM c JOIN d ON c.one = d.one",
        "SELECT 'ǆ' AS lowercase_digraph FROM t",
    ],
)
def test_upper_case_input_stream_matches_the_char_stream_wrapper(query):
    """The up-front upper-casing gives the same parse as the per-character wrapper."""
    wrapped = build_parser(InputStream(query), early_bail=False)
    upfront = build_string_parser(query, early_bail=False)
    assert _tree_string(upfront) == _tree_string(wrapped)


def test_upper_case_input_stream_keeps_the_original_text():
    """Keywords match in any case, but tokens keep the spelling they were written with."""
    query = parse("select Foo, bAr from Some_Table where Foo = 'MiXeD'")

    names = {name.name for name in query.find_all(ast.Name)}
    assert {"Foo", "bAr", "Some_Table"} <= names
    assert "'MiXeD'" in str(query)


def test_upper_case_input_stream_handles_characters_that_upper_case_to_many():
    """`ß` upper-cases to `SS`; positions must not shift for what follows it."""
    stream = UpperCaseInputStream("ß select")
    assert len(stream.data) == len("ß select")
    assert stream.getText(0, 0) == "ß"
    assert stream.getText(2, 7) == "select"


def test_upper_case_input_stream_empty_interval_has_no_text():
    stream = UpperCaseInputStream("")
    assert stream.getText(SimpleNamespace(a=0, b=-1)) == ""


def test_strict_mode_stays_case_sensitive():
    stream = build_string_parser("select 1", strict_mode=True).getInputStream()
    assert not isinstance(stream, UpperCaseInputStream)
