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
            from_=From(table=Table(name="a", quote_style=None), joins=[]),
            group_by=[],
            having=None,
            projection=[Wildcard()],
            where=None,
            limit=None,
        ),
        subquery=False,
    )
