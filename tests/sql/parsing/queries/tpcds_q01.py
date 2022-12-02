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
            distinct=False,
            from_=From(
                table=Alias(
                    name="ctr1",
                    quote_style="",
                    child=Table(name="customer_total_return", quote_style=""),
                ),
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(name="store", quote_style=""),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(name="s_store_sk", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="ctr_store_sk", quote_style=""),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(name="s_state", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="ctr_customer_sk", quote_style=""),
                                op=BinaryOpKind.Eq,
                                right=Column(name="c_customer_sk", quote_style=""),
                            ),
                        ),
                    ),
                    Join(
                        kind=JoinKind.LeftOuter,
                        table=Table(name="customer", quote_style=""),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(name="s_store_sk", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="ctr_store_sk", quote_style=""),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(name="s_state", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="ctr_customer_sk", quote_style=""),
                                op=BinaryOpKind.Eq,
                                right=Column(name="c_customer_sk", quote_style=""),
                            ),
                        ),
                    ),
                ],
            ),
            group_by=[],
            having=None,
            projection=[Column(name="c_customer_id", quote_style="")],
            where=BinaryOp(
                left=BinaryOp(
                    left=BinaryOp(
                        left=Column(name="ctr_total_return", quote_style=""),
                        op=BinaryOpKind.Gt,
                        right=Query(
                            select=Select(
                                distinct=True,
                                from_=From(
                                    table=Alias(
                                        name="ctr2",
                                        quote_style="",
                                        child=Table(
                                            name="customer_total_return",
                                            quote_style="",
                                        ),
                                    ),
                                    joins=[],
                                ),
                                group_by=[],
                                having=None,
                                projection=[
                                    BinaryOp(
                                        left=Function(
                                            name="Avg",
                                            quote_style="",
                                            args=[
                                                Column(
                                                    name="ctr_total_return",
                                                    quote_style="",
                                                ),
                                            ],
                                        ),
                                        op=BinaryOpKind.Multiply,
                                        right=Number(value=1.2),
                                    ),
                                ],
                                where=BinaryOp(
                                    left=Column(name="ctr_store_sk", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="ctr_store_sk", quote_style=""),
                                ),
                                limit=None,
                            ),
                            ctes=[],
                        ),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(name="s_store_sk", quote_style=""),
                        op=BinaryOpKind.Eq,
                        right=Column(name="ctr_store_sk", quote_style=""),
                    ),
                ),
                op=BinaryOpKind.Or,
                right=BinaryOp(
                    left=BinaryOp(
                        left=Column(name="s_state", quote_style=""),
                        op=BinaryOpKind.NotEq,
                        right=String(value="TN"),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(name="ctr_customer_sk", quote_style=""),
                        op=BinaryOpKind.Eq,
                        right=Column(name="c_customer_sk", quote_style=""),
                    ),
                ),
            ),
            limit=Number(value=100),
        ),
        ctes=[
            Alias(
                name="customer_total_return",
                quote_style="",
                child=Select(
                    distinct=False,
                    from_=From(
                        table=Table(name="store_returns", quote_style=""),
                        joins=[
                            Join(
                                kind=JoinKind.Inner,
                                table=Table(name="date_dim", quote_style=""),
                                on=BinaryOp(
                                    left=Column(name="sr_customer_sk", quote_style=""),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="d_date_sk", quote_style=""),
                                ),
                            ),
                        ],
                    ),
                    group_by=[
                        Column(name="sr_customer_sk", quote_style=""),
                        Column(name="sr_store_sk", quote_style=""),
                    ],
                    having=None,
                    projection=[
                        Alias(
                            name="ctr_customer_sk",
                            quote_style="",
                            child=Column(name="sr_customer_sk", quote_style=""),
                        ),
                        Alias(
                            name="ctr_store_sk",
                            quote_style="",
                            child=Column(name="sr_store_sk", quote_style=""),
                        ),
                        Alias(
                            name="ctr_total_return",
                            quote_style="",
                            child=Function(
                                name="Sum",
                                quote_style="",
                                args=[Column(name="sr_return_amt", quote_style="")],
                            ),
                        ),
                    ],
                    where=BinaryOp(
                        left=BinaryOp(
                            left=Column(name="sr_returned_date_sk", quote_style=""),
                            op=BinaryOpKind.Eq,
                            right=Column(name="d_date_sk", quote_style=""),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(name="d_year", quote_style=""),
                            op=BinaryOpKind.Eq,
                            right=Number(value=2001),
                        ),
                    ),
                    limit=None,
                ),
            ),
        ],
    ).compile_parents()
