"""
tests for the backend that takes sqloxide output and transforms it into an DJ ast
"""
import pytest

from dj.sql.parsing.backends.sqloxide import parse
from dj.sql.parsing.backends.exceptions import DJParseException
from tests.sql.utils import TPCDS_QUERY_SET, read_query


@pytest.mark.parametrize("query_name", TPCDS_QUERY_SET)
def test_sqloxide_parse_tpcds(request, query_name):
    """
    test tpcds queries parse properly
    """
    expected_ast = request.getfixturevalue(query_name)
    query = read_query(f"{query_name}.sql")
    parsed = parse(query)
    assert expected_ast.compare(parsed)


def test_multi_statement_exception():
    """
    tests we only will parse a single sql statement at a time
    """
    with pytest.raises(DJParseException):
        parse("select x from a; select x from b;")


def test_union_exception():
    """
    tests that unions are currently unsupported
    """
    with pytest.raises(DJParseException):
        parse("select x from a union select x from b")


def test_function_more_than_one_compound_ident_exception():
    """
    tests that a function identified with more than one ident in a compound identifier fails
    """
    with pytest.raises(DJParseException):
        parse("select s.um(x) from a")


def test_table_more_than_one_compound_ident_exception():
    """
    tests that a table identified with more than one idents in a compound identifier fails
    """
    with pytest.raises(DJParseException):
        parse("select x from a.b")


def test_column_more_than_two_compound_ident_exception():
    """
    tests that a column identified with more than two idents in a compound identifier fails
    """
    with pytest.raises(DJParseException):
        parse("select x.y.z from a")


def test_multiple_from_exception():
    """
    tests that implicit joins fails
    """
    with pytest.raises(DJParseException):
        parse("select * from a, b")
