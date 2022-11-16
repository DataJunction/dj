"""
Helper functions.
"""
import os

from collections import Counter
from sqlalchemy.sql import Select

TPCDS_QUERY_SET = ["tpcds_q01", "tpcds_q99"]


def query_to_string(query: Select) -> str:
    """
    Helper function to compile a SQLAlchemy query to a string.
    """
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def compare_query_strings(str1, str2: str) -> bool:
    """
    compare two query strings based on sorted tokens
    """
    import re

    ignore = {"as", "\n", "\t", " ", "(", ")", ";"}
    counted1 = Counter(
        "".join(
            token
            for token in re.sub(r"[\(\)]", " ", str1.lower()).split()
            if token not in ignore
        )
    )
    counted2 = Counter(
        "".join(
            token
            for token in re.sub(r"[\(\)]", " ", str2.lower()).split()
            if token not in ignore
        )
    )
    return not [k for k in (counted1 - counted2).keys() - ignore] + [
        k for k in (counted2 - counted1).keys() - ignore
    ]


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
