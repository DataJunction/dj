"""
Helper functions.
"""
import os

from sqlalchemy.sql import Select

from dj.sql.parsing.backends.sqloxide import parse
from dj.sql.parsing.frontends.ansi_string import sql

TPCDS_QUERY_SET = ["tpcds_q01", "tpcds_q99"]


def query_to_string(query: Select) -> str:
    """
    Helper function to compile a SQLAlchemy query to a string.
    """
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def compare_query_strings(str1, str2: str) -> bool:
    """
    compare two query strings
    """
    return parse(sql(parse(str1))).compare(parse(sql(parse(str2))))


def read_query(name: str) -> str:
    """
    Read a tpcds query given filename e.g. tpcds_q01.sql
    """
    with open(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "parsing",
            "queries",
            name,
        ),
        encoding="utf-8",
    ) as file:
        return file.read()
