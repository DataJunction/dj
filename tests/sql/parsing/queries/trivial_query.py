"""
a trivial query
"""
import pytest

from dj.sql.parsing.ast import From, Name, Query, Select, Table, Wildcard


@pytest.fixture
def trivial_query():
    """
    a trivial select * from a query
    """
    return Query(
        select=Select(
            from_=From(
                tables=[Table(name=Name(name="a", quote_style=""), namespace=None)],
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[Wildcard()],
            where=None,
            limit=None,
            distinct=False,
        ),
        ctes=[],
        dialect="ansi",
    )
