"""
a trivial query
"""
import pytest

from dj.sql.parsing.ast import From, Query, Select, Table, Wildcard


@pytest.fixture
def trivial_query():
    """
    a trivial select * from a query
    """
    return Query(
        ctes=[],
        select=Select(
            distinct=False,
            from_=From(table=Table(name="a")),
            projection=[Wildcard()],
        ),
    ).compile_parents()
