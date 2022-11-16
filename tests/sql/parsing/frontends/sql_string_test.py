"""
tests for DJ ast representation as sql string
"""
import pytest

from dj.sql.parsing.frontends.sql_string import sql
from tests.sql.utils import TPCDS_QUERY_SET, compare_query_strings, read_query


def test_trivial_sql_string(trivial_query):
    """
    test converting a trivial query to sql string
    """
    assert compare_query_strings(
        sql(trivial_query).strip(),
        read_query("trivial_query.sql"),
    )


@pytest.mark.parametrize("query_name", TPCDS_QUERY_SET)
def test_sql_string_tpcds(request, query_name):
    """
    test turning sql queries into strings via the string frontend
    """
    gen_sql = sql(request.getfixturevalue(query_name))
    query = read_query(f"{query_name}.sql")
    assert compare_query_strings(gen_sql, query)


@pytest.mark.parametrize("value", (1, "hello", 3.14, {"x": "y"}, [1, 2, 3]))
def test_only_node(value):
    """
    test non node values to make sure we don't convert them to sql by mistake
    """
    with pytest.raises(Exception):
        sql(value)
