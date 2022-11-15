"""
tests for DJ ast representation as sql string
"""
import pytest
from sqloxide import parse_sql

from dj.sql.parsing.frontends.sql_string import sql
from tests.sql.utils import TPCDS_QUERY_SET, read_query


@pytest.mark.parametrize("query_name", TPCDS_QUERY_SET)
def test_sql_string_tpcds_q01(request, query_name):
    """
    test turning sql queries into strings via the string frontend
    """
    gen_sql = sql(request.getfixturevalue(query_name))
    sqloxide_parsed_gen_sql = parse_sql(gen_sql, "ansi")
    query = read_query(f"{query_name}.sql")
    sqloxide_parsed_sql = parse_sql(query, "ansi")
    assert sqloxide_parsed_gen_sql == sqloxide_parsed_sql
