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
    Name,
    Namespace,
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
            from_=From(
                tables=[
                    Alias(
                        name=Name(name="t1", quote_style=""),
                        namespace=None,
                        child=Query(
                            select=Select(
                                from_=From(
                                    tables=[
                                        Table(
                                            name=Name(name="t", quote_style=""),
                                            namespace=None,
                                        ),
                                    ],
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
                        ),
                    ),
                ],
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(
                            name=Name(name="t2", quote_style=""),
                            namespace=None,
                        ),
                        on=BinaryOp(
                            left=Column(
                                name=Name(name="c", quote_style=""),
                                namespace=Namespace(
                                    names=[Name(name="t1", quote_style="")],
                                ),
                            ),
                            op=BinaryOpKind.Eq,
                            right=Column(
                                name=Name(name="c", quote_style=""),
                                namespace=Namespace(
                                    names=[Name(name="t2", quote_style="")],
                                ),
                            ),
                        ),
                    ),
                ],
            ),
            group_by=[],
            having=None,
            projection=[
                Column(
                    name=Name(name="a", quote_style=""),
                    namespace=Namespace(names=[Name(name="t1", quote_style="")]),
                ),
                Column(
                    name=Name(name="b", quote_style=""),
                    namespace=Namespace(names=[Name(name="t2", quote_style="")]),
                ),
            ],
            where=None,
            limit=None,
            distinct=False,
        ),
        ctes=[],
    )
