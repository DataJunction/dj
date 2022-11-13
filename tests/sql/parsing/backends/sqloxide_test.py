"""
tests for the backend that takes sqloxide output and transforms it into an DJ ast
"""

from dj.sql.parsing.ast import Query
from dj.sql.parsing.backends.sqloxide import parse

from ...utils import read_query


def test_sqloxide_parse_tpcds_q01(tpcds_q01: Query):
    query = read_query("tcpds_q01.sql")
    parsed = parse(query)
    assert tpcds_q01.compare(parsed)
