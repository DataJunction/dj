"""
fixtures for derived_subquery.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    Case,
    Column,
    From,
    Identifier,
    IsNull,
    Name,
    Query,
    Select,
    Table,
    UnaryOp,
    UnaryOpKind,
)


@pytest.fixture
def case_when_null():
    """
    dj ast for case when null
    """
    return Query(
        select=Select(
            distinct=False,
            from_=From(
                table=Alias(
                    ident=Identifier(idents=[Name(name="web", quote_style="")]),
                    child=Table(
                        ident=Identifier(idents=[Name(name="web_v1", quote_style="")]),
                    ),
                ),
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[
                Alias(
                    ident=Identifier(idents=[Name(name="item_sk", quote_style="")]),
                    child=Case(
                        conditions=[
                            IsNull(
                                expr=Column(
                                    ident=Identifier(
                                        idents=[
                                            Name(name="web", quote_style=""),
                                            Name(name="item_sk", quote_style=""),
                                        ],
                                    ),
                                ),
                            ),
                        ],
                        else_result=Column(
                            ident=Identifier(
                                idents=[
                                    Name(name="store", quote_style=""),
                                    Name(name="item_sk", quote_style=""),
                                ],
                            ),
                        ),
                        operand=None,
                        results=[
                            Column(
                                ident=Identifier(
                                    idents=[
                                        Name(name="web", quote_style=""),
                                        Name(name="item_sk", quote_style=""),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
                Alias(
                    ident=Identifier(idents=[Name(name="d_date", quote_style="")]),
                    child=Case(
                        conditions=[
                            UnaryOp(
                                op=UnaryOpKind.Not,
                                expr=IsNull(
                                    expr=Column(
                                        ident=Identifier(
                                            idents=[
                                                Name(name="web", quote_style=""),
                                                Name(name="d_date", quote_style=""),
                                            ],
                                        ),
                                    ),
                                ),
                            ),
                        ],
                        else_result=Column(
                            ident=Identifier(
                                idents=[
                                    Name(name="store", quote_style=""),
                                    Name(name="d_date", quote_style=""),
                                ],
                            ),
                        ),
                        operand=None,
                        results=[
                            Column(
                                ident=Identifier(
                                    idents=[
                                        Name(name="web", quote_style=""),
                                        Name(name="d_date", quote_style=""),
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
                Alias(
                    ident=Identifier(idents=[Name(name="web_sales", quote_style="")]),
                    child=Column(
                        ident=Identifier(
                            idents=[
                                Name(name="web", quote_style=""),
                                Name(name="cume_sales", quote_style=""),
                            ],
                        ),
                    ),
                ),
                Alias(
                    ident=Identifier(idents=[Name(name="store_sales", quote_style="")]),
                    child=Column(
                        ident=Identifier(
                            idents=[
                                Name(name="store", quote_style=""),
                                Name(name="cume_sales", quote_style=""),
                            ],
                        ),
                    ),
                ),
            ],
            where=None,
            limit=None,
        ),
        ctes=[],
    ).compile_parents()
