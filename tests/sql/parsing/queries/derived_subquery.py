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
    Identifier,
    Join,
    JoinKind,
    Name,
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
                    ident=Identifier(idents=[Name(name="t1", quote_style="")]),
                    child=Query(
                        select=Select(
                            distinct=False,
                            from_=From(
                                table=Table(
                                    ident=Identifier(
                                        idents=[Name(name="t", quote_style="")],
                                    ),
                                ),
                                joins=[],
                            ),
                            group_by=[],
                            having=None,
                            projection=[Wildcard()],
                            where=None,
                            limit=None,
                        ),
                        ctes=[],
                    ),
                ),
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(
                            ident=Identifier(idents=[Name(name="t2", quote_style="")]),
                        ),
                        on=BinaryOp(
                            left=Column(
                                ident=Identifier(
                                    idents=[
                                        Name(name="t1", quote_style=""),
                                        Name(name="c", quote_style=""),
                                    ],
                                ),
                            ),
                            op=BinaryOpKind.Eq,
                            right=Column(
                                ident=Identifier(
                                    idents=[
                                        Name(name="t2", quote_style=""),
                                        Name(name="c", quote_style=""),
                                    ],
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
                    ident=Identifier(
                        idents=[
                            Name(name="t1", quote_style=""),
                            Name(name="a", quote_style=""),
                        ],
                    ),
                ),
                Column(
                    ident=Identifier(
                        idents=[
                            Name(name="t2", quote_style=""),
                            Name(name="b", quote_style=""),
                        ],
                    ),
                ),
            ],
            where=None,
            limit=None,
        ),
        ctes=[],
    ).compile_parents()
