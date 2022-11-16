"""
tests for DJ ast representation as sql string
"""
import pytest

from dj.sql.parsing.frontends.sql_string import sql
from tests.sql.utils import TPCDS_QUERY_SET, read_query, compare_query_strings
from dj.sql.parsing.ast import From, Query, Select, Table, Wildcard


def test_trivial():
    assert (
        sql(
            Query(
                ctes=[],
                select=Select(
                    distinct=False,
                    from_=From(table=Table(name="a", quote_style=None), joins=[]),
                    group_by=[],
                    having=None,
                    projection=[Wildcard()],
                    where=None,
                    limit=None,
                ),
                subquery=False,
            )
        )
        == """SELECT *
FROM a
"""
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
    with pytest.raises(Exception):
        sql(value)
