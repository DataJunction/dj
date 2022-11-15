"""
tests for the backend that takes sqloxide output and transforms it into an DJ ast
"""
import pytest

from dj.sql.parsing.backends.sqloxide import parse
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
