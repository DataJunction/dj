"""
a trivial query
"""
import pytest

from dj.sql.parsing.ast import From, Identifier, Name, Query, Select, Table, Wildcard


@pytest.fixture
def trivial_query():
    """
    a trivial select * from a query
    """
    return Query(
        select=Select(
            distinct=False,
            from_=From(
                table=Table(ident=Identifier(idents=[Name(name="a", quote_style="")])),
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[Wildcard()],
            where=None,
            limit=None,
        ),
        ctes=[],
    ).compile_parents()
