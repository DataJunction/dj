"""
Helper functions.
"""
import os

from sqlalchemy.sql import Select

from dj.sql.parsing import ast
from dj.sql.parsing.backends.antlr4 import parse

TPCDS_QUERY_SET = ["tpcds_q01", "tpcds_q99"]


def query_to_string(query: Select) -> str:
    """
    Helper function to compile a SQLAlchemy query to a string.
    """
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def compare_query_strings(str1: str, str2: str) -> bool:
    """
    compare two query strings
    """
    query1 = parse(str1)
    query1.select.projection = sorted(
        query1.select.projection,
        key=lambda x: str(x.alias_or_name),  # type: ignore
    )[:]
    query2 = parse(str2)
    query2.select.projection = sorted(
        query2.select.projection,
        key=lambda x: str(x.alias_or_name),  # type: ignore
    )[:]
    for relation in query1.find_all(ast.Relation):
        relation.extensions = sorted(
            relation.extensions,
            key=lambda ext: str(ext.right.alias_or_name),  # type: ignore
        )
    for relation in query2.find_all(ast.Relation):
        relation.extensions = sorted(
            relation.extensions,
            key=lambda ext: str(ext.right.alias_or_name),  # type: ignore
        )
    return parse(str(query1)).compare(parse(str(query2)))


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
