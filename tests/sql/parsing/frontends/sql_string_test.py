"""
tests for DJ ast representation as sql string
"""
from dj.sql.parsing.frontends.sql_string import sql
from dj.sql.parsing.ast import Query
from sqloxide import parse_sql
from ...utils import read_query

def test_sql_string_tpcds_q01(tcpds_q01: Query):
    gen_sql = sql(tcpds_q01)
    sqloxide_parsed_gen_sql = parse_sql(gen_sql, 'ansi')
    query = read_query("tcpds_q01.sql")
    sqloxide_parsed_sql = parse_sql(query, 'ansi')
    assert sqloxide_parsed_gen_sql == sqloxide_parsed_sql