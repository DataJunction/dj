"""
fixtures for tpcds_q01.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    BinaryOp,
    BinaryOpKind,
    Column,
    From,
    Function,
    Join,
    JoinKind,
    Name,
    Namespace,
    Number,
    Query,
    Select,
    String,
    Table,
)


@pytest.fixture
def tpcds_q01():
    """
    dj ast for tpcds query 1
    """
    return Query(
        select=Select(
            from_=From(
                tables=[
                    Alias(
                        name=Name(name="ctr1", quote_style=""),
                        namespace=None,
                        child=Table(
                            name=Name(name="customer_total_return", quote_style=""),
                            namespace=None,
                        ),
                    ),
                ],
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(
                            name=Name(name="store", quote_style=""),
                            namespace=None,
                        ),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(
                                        name=Name(name="s_store_sk", quote_style=""),
                                        namespace=None,
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=Column(
                                        name=Name(name="ctr_store_sk", quote_style=""),
                                        namespace=Namespace(
                                            names=[Name(name="ctr1", quote_style="")],
                                        ),
                                    ),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(
                                        name=Name(name="s_state", quote_style=""),
                                        namespace=None,
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(
                                    name=Name(name="ctr_customer_sk", quote_style=""),
                                    namespace=Namespace(
                                        names=[Name(name="ctr1", quote_style="")],
                                    ),
                                ),
                                op=BinaryOpKind.Eq,
                                right=Column(
                                    name=Name(name="c_customer_sk", quote_style=""),
                                    namespace=None,
                                ),
                            ),
                        ),
                    ),
                    Join(
                        kind=JoinKind.LeftOuter,
                        table=Table(
                            name=Name(name="customer", quote_style=""),
                            namespace=None,
                        ),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(
                                        name=Name(name="s_store_sk", quote_style=""),
                                        namespace=None,
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=Column(
                                        name=Name(name="ctr_store_sk", quote_style=""),
                                        namespace=Namespace(
                                            names=[Name(name="ctr1", quote_style="")],
                                        ),
                                    ),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(
                                        name=Name(name="s_state", quote_style=""),
                                        namespace=None,
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(
                                    name=Name(name="ctr_customer_sk", quote_style=""),
                                    namespace=Namespace(
                                        names=[Name(name="ctr1", quote_style="")],
                                    ),
                                ),
                                op=BinaryOpKind.Eq,
                                right=Column(
                                    name=Name(name="c_customer_sk", quote_style=""),
                                    namespace=None,
                                ),
                            ),
                        ),
                    ),
                ],
            ),
            group_by=[],
            having=None,
            projection=[
                Column(name=Name(name="c_customer_id", quote_style=""), namespace=None),
            ],
            where=BinaryOp(
                left=BinaryOp(
                    left=BinaryOp(
                        left=Column(
                            name=Name(name="ctr_total_return", quote_style=""),
                            namespace=Namespace(
                                names=[Name(name="ctr1", quote_style="")],
                            ),
                        ),
                        op=BinaryOpKind.Gt,
                        right=Query(  # pylint: disable=W0212
                            select=Select(
                                from_=From(
                                    tables=[
                                        Alias(
                                            name=Name(name="ctr2", quote_style=""),
                                            namespace=None,
                                            child=Table(
                                                name=Name(
                                                    name="customer_total_return",
                                                    quote_style="",
                                                ),
                                                namespace=None,
                                            ),
                                        ),
                                    ],
                                    joins=[],
                                ),
                                group_by=[],
                                having=None,
                                projection=[
                                    BinaryOp(
                                        left=Function(
                                            name=Name(name="Avg", quote_style=""),
                                            namespace=Namespace(names=[]),
                                            args=[
                                                Column(
                                                    name=Name(
                                                        name="ctr_total_return",
                                                        quote_style="",
                                                    ),
                                                    namespace=None,
                                                ),
                                            ],
                                        ),
                                        op=BinaryOpKind.Multiply,
                                        right=Number(value=1.2),
                                    ),
                                ],
                                where=BinaryOp(
                                    left=Column(
                                        name=Name(name="ctr_store_sk", quote_style=""),
                                        namespace=Namespace(
                                            names=[Name(name="ctr1", quote_style="")],
                                        ),
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=Column(
                                        name=Name(name="ctr_store_sk", quote_style=""),
                                        namespace=Namespace(
                                            names=[Name(name="ctr2", quote_style="")],
                                        ),
                                    ),
                                ),
                                limit=None,
                                distinct=True,
                            ),
                            ctes=[],
                        )._to_select(),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(
                            name=Name(name="s_store_sk", quote_style=""),
                            namespace=None,
                        ),
                        op=BinaryOpKind.Eq,
                        right=Column(
                            name=Name(name="ctr_store_sk", quote_style=""),
                            namespace=Namespace(
                                names=[Name(name="ctr1", quote_style="")],
                            ),
                        ),
                    ),
                ),
                op=BinaryOpKind.Or,
                right=BinaryOp(
                    left=BinaryOp(
                        left=Column(
                            name=Name(name="s_state", quote_style=""),
                            namespace=None,
                        ),
                        op=BinaryOpKind.NotEq,
                        right=String(value="TN"),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(
                            name=Name(name="ctr_customer_sk", quote_style=""),
                            namespace=Namespace(
                                names=[Name(name="ctr1", quote_style="")],
                            ),
                        ),
                        op=BinaryOpKind.Eq,
                        right=Column(
                            name=Name(name="c_customer_sk", quote_style=""),
                            namespace=None,
                        ),
                    ),
                ),
            ),
            limit=Number(value=100),
            distinct=False,
        ),
        ctes=[
            Alias(
                name=Name(name="customer_total_return", quote_style=""),
                namespace=None,
                child=Select(
                    from_=From(
                        tables=[
                            Table(
                                name=Name(name="store_returns", quote_style=""),
                                namespace=None,
                            ),
                        ],
                        joins=[
                            Join(
                                kind=JoinKind.Inner,
                                table=Table(
                                    name=Name(name="date_dim", quote_style=""),
                                    namespace=None,
                                ),
                                on=BinaryOp(
                                    left=Column(
                                        name=Name(
                                            name="sr_customer_sk",
                                            quote_style="",
                                        ),
                                        namespace=Namespace(
                                            names=[
                                                Name(
                                                    name="store_returns",
                                                    quote_style="",
                                                ),
                                            ],
                                        ),
                                    ),
                                    op=BinaryOpKind.Eq,
                                    right=Column(
                                        name=Name(name="d_date_sk", quote_style=""),
                                        namespace=Namespace(
                                            names=[
                                                Name(name="date_dim", quote_style=""),
                                            ],
                                        ),
                                    ),
                                ),
                            ),
                        ],
                    ),
                    group_by=[
                        Column(
                            name=Name(name="sr_customer_sk", quote_style=""),
                            namespace=None,
                        ),
                        Column(
                            name=Name(name="sr_store_sk", quote_style=""),
                            namespace=None,
                        ),
                    ],
                    having=None,
                    projection=[
                        Alias(
                            name=Name(name="ctr_customer_sk", quote_style=""),
                            namespace=None,
                            child=Column(
                                name=Name(name="sr_customer_sk", quote_style=""),
                                namespace=None,
                            ),
                        ),
                        Alias(
                            name=Name(name="ctr_store_sk", quote_style=""),
                            namespace=None,
                            child=Column(
                                name=Name(name="sr_store_sk", quote_style=""),
                                namespace=None,
                            ),
                        ),
                        Alias(
                            name=Name(name="ctr_total_return", quote_style=""),
                            namespace=None,
                            child=Function(
                                name=Name(name="Sum", quote_style=""),
                                namespace=Namespace(names=[]),
                                args=[
                                    Column(
                                        name=Name(name="sr_return_amt", quote_style=""),
                                        namespace=None,
                                    ),
                                ],
                            ),
                        ),
                    ],
                    where=BinaryOp(
                        left=BinaryOp(
                            left=Column(
                                name=Name(name="sr_returned_date_sk", quote_style=""),
                                namespace=None,
                            ),
                            op=BinaryOpKind.Eq,
                            right=Column(
                                name=Name(name="d_date_sk", quote_style=""),
                                namespace=None,
                            ),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(
                                name=Name(name="d_year", quote_style=""),
                                namespace=None,
                            ),
                            op=BinaryOpKind.Eq,
                            right=Number(value=2001),
                        ),
                    ),
                    limit=None,
                    distinct=False,
                ),
            ),
        ],
    )
