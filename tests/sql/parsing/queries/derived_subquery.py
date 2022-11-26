"""
fixtures for derived_subquery.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    BinaryOp,
    BinaryOpKind,
    Column,
    From,
    Join,
    JoinKind,
    Query,
    Select,
    Table,
    Wildcard,
)


@pytest.fixture
def derived_subquery():
    """
    dj ast for derived_subquery
    """
    return Query(
        select=Select(
            distinct=False,
            from_=From(
                table=Alias(
                    name="t1",
                    child=Query(
                        select=Select(
                            distinct=False,
                            from_=From(table=Table(name="t")),
                            projection=[Wildcard()],
                        ),
                        ctes=[],
                    ),
                ),
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(name="t2"),
                        on=BinaryOp(
                            left=Column(name="c"),
                            op=BinaryOpKind.Eq,
                            right=Column(name="c"),
                        ),
                    ),
                ],
            ),
            projection=[Column(name="a"), Column(name="b")],
        ),
    ).compile_parents()
