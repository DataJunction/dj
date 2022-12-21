"""
fixtures for derived_subquery.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    Case,
    Column,
    From,
    IsNull,
    Name,
    Namespace,
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
            from_=From(
                table=Alias(
                    name=Name(name="web", quote_style=""),
                    namespace=None,
                    child=Table(
                        name=Name(name="web_v1", quote_style=""),
                        namespace=None,
                    ),
                ),
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[
                Alias(
                    name=Name(name="item_sk", quote_style=""),
                    namespace=None,
                    child=Case(
                        conditions=[
                            IsNull(
                                expr=Column(
                                    name=Name(name="item_sk", quote_style=""),
                                    namespace=Namespace(
                                        names=[Name(name="web", quote_style="")],
                                    ),
                                ),
                            ),
                        ],
                        else_result=Column(
                            name=Name(name="item_sk", quote_style=""),
                            namespace=Namespace(
                                names=[Name(name="store", quote_style="")],
                            ),
                        ),
                        operand=None,
                        results=[
                            Column(
                                name=Name(name="item_sk", quote_style=""),
                                namespace=Namespace(
                                    names=[Name(name="web", quote_style="")],
                                ),
                            ),
                        ],
                    ),
                ),
                Alias(
                    name=Name(name="d_date", quote_style=""),
                    namespace=None,
                    child=Case(
                        conditions=[
                            UnaryOp(
                                op=UnaryOpKind.Not,
                                expr=IsNull(
                                    expr=Column(
                                        name=Name(name="d_date", quote_style=""),
                                        namespace=Namespace(
                                            names=[Name(name="web", quote_style="")],
                                        ),
                                    ),
                                ),
                            ),
                        ],
                        else_result=Column(
                            name=Name(name="d_date", quote_style=""),
                            namespace=Namespace(
                                names=[Name(name="store", quote_style="")],
                            ),
                        ),
                        operand=None,
                        results=[
                            Column(
                                name=Name(name="d_date", quote_style=""),
                                namespace=Namespace(
                                    names=[Name(name="web", quote_style="")],
                                ),
                            ),
                        ],
                    ),
                ),
                Alias(
                    name=Name(name="web_sales", quote_style=""),
                    namespace=None,
                    child=Column(
                        name=Name(name="cume_sales", quote_style=""),
                        namespace=Namespace(names=[Name(name="web", quote_style="")]),
                    ),
                ),
                Alias(
                    name=Name(name="store_sales", quote_style=""),
                    namespace=None,
                    child=Column(
                        name=Name(name="cume_sales", quote_style=""),
                        namespace=Namespace(names=[Name(name="store", quote_style="")]),
                    ),
                ),
            ],
            where=None,
            limit=None,
            distinct=False,
        ),
        ctes=[],
    )
