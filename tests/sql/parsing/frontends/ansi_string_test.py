"""
tests for DJ ast representation as sql string
"""
import pytest

from dj.sql.parsing import parse
from dj.sql.parsing.ast import (
    Alias,
    Column,
    From,
    In,
    Name,
    Namespace,
    Number,
    Query,
    Select,
    Table,
)
from dj.sql.parsing.frontends.string import sql
from tests.sql.utils import TPCDS_QUERY_SET, compare_query_strings, read_query


def test_unnaceptable_dialect():
    """
    test that a dialect that we do not know raises
    """
    with pytest.raises(ValueError):
        sql(Name("x"), "Unacceptable")


def test_namespaced_table():
    """
    test namespaced table to string
    """
    assert (
        str(Table(Name("a")).add_namespace(Namespace([Name("db"), Name("schema")])))
        == "db.schema.a"
    )


def test_aliased_table_column():
    """
    test namespaced column to string
    """
    table = Table(Name("tbl"))
    aliased = Alias(Name("a"), child=table)
    col = Column(Name("x"), _table=aliased)
    assert str(col) == "a.x"


def test_in_str():
    """
    test an IN to a string
    """
    in_ = In(Column(Name("x")), [Number(5)], True)
    assert str(in_) == "x NOT IN (5)"


def test_over_string():
    """
    test converting a window function
    """
    raw = """
        SELECT duration_seconds,
        SUM(duration_seconds) OVER (ORDER BY start_time) AS running_total
    FROM tutorial.dc_bikeshare_q1_2012
    """
    ast = parse(raw)
    assert compare_query_strings(sql(ast).strip(), raw)


def test_select_with_orderby_string():
    """
    test converting a select with order by
    """
    raw = """
    SELECT * FROM (
        SELECT ID, GEOM, Name
        FROM t
        ORDER BY Name
        ) as tbl
    """
    ast = parse(raw)
    assert compare_query_strings(sql(ast).strip(), raw)


def test_case_when_null_sql_string(case_when_null):
    """
    test converting a case_when_null query to sql string
    """
    assert compare_query_strings(
        sql(case_when_null).strip(),
        read_query("case_when_null.sql"),
    )


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
    query = read_query(f"{query_name}.sql")
    gen_sql = sql(request.getfixturevalue(query_name))
    assert compare_query_strings(gen_sql, query)


def test_column_table_eq_compound_ident():
    """tests to see if marking a column as belonging to a table
    returns the same thing as a column with a compound identifier
    """
    assert sql(
        Query(
            select=Select(
                distinct=False,
                from_=From(
                    tables=[
                        Table(
                            Name(name="a", quote_style=""),
                        ),
                    ],
                    joins=[],
                ),
                group_by=[],
                having=None,
                projection=[
                    Column(
                        Name(name="x", quote_style=""),
                    ).add_namespace(Namespace([Name("a")])),
                ],
                where=None,
                limit=None,
            ),
            ctes=[],
        ),
    ) == sql(
        Query(
            select=Select(
                distinct=False,
                from_=From(
                    tables=[
                        Table(
                            Name(name="a", quote_style=""),
                        ),
                    ],
                    joins=[],
                ),
                group_by=[],
                having=None,
                projection=[
                    Column(Name(name="x", quote_style="")).add_table(
                        Table(
                            Name(name="a", quote_style=""),
                        ),
                    ),
                ],
                where=None,
                limit=None,
            ),
            ctes=[],
        ),
    )
