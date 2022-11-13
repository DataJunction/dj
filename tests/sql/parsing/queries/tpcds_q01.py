"""
fixtures for tpcds_q01.sql
"""


import pytest
from dj.sql.parsing.ast import (
    Node,
    Alias,
    Value,
    Column,
    Table,
    From,
    Function,
    Select,
    String,
    Query,
    BinaryOp,
    Case,
    Join,
    UnaryOp,
    Wildcard,
    BinaryOpKind,
    JoinKind
)

@pytest.fixture(scope="session")
def tpcds_q01():
    return Query(
        ctes=[
            Alias(
                name="customer_total_return",
                quote_style=None,
                child=Select(
                    distinct=False,
                    from_=From(
                        table=Table(name="store_returns", quote_style=None),
                        joins=[
                            Join(
                                kind=JoinKind.Inner,
                                table=Table(name="date_dim", quote_style=None),
                                on=BinaryOp(
                                    left=Column(name="sr_customer_sk", quote_style=None),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="d_date_sk", quote_style=None),
                                ),
                            )
                        ],
                    ),
                    group_by=[
                        Column(name="sr_customer_sk", quote_style=None),
                        Column(name="sr_store_sk", quote_style=None),
                    ],
                    having=None,
                    projection=[
                        Alias(
                            name="ctr_customer_sk",
                            quote_style=None,
                            child=Column(name="sr_customer_sk", quote_style=None),
                        ),
                        Alias(
                            name="ctr_store_sk",
                            quote_style=None,
                            child=Column(name="sr_store_sk", quote_style=None),
                        ),
                        Alias(
                            name="ctr_total_return",
                            quote_style=None,
                            child=Function(
                                name="Sum",
                                quote_style=None,
                                args=[Column(name="sr_return_amt", quote_style=None)],
                            ),
                        ),
                    ],
                    where=BinaryOp(
                        left=BinaryOp(
                            left=Column(name="sr_returned_date_sk", quote_style=None),
                            op=BinaryOpKind.Eq,
                            right=Column(name="d_date_sk", quote_style=None),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(name="d_year", quote_style=None),
                            op=BinaryOpKind.Eq,
                            right=Number(value="2001"),
                        ),
                    ),
                    limit=None,
                ),
            )
        ],
        select=Select(
            distinct=False,
            from_=From(
                table=Alias(
                    name="ctr1",
                    quote_style=None,
                    child=Table(name="customer_total_return", quote_style=None),
                ),
                joins=[
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(name="store", quote_style=None),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(name="s_store_sk", quote_style=None),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="ctr_store_sk", quote_style=None),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(name="s_state", quote_style=None),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="ctr_customer_sk", quote_style=None),
                                op=BinaryOpKind.Eq,
                                right=Column(name="c_customer_sk", quote_style=None),
                            ),
                        ),
                    ),
                    Join(
                        kind=JoinKind.Inner,
                        table=Table(name="customer", quote_style=None),
                        on=BinaryOp(
                            left=BinaryOp(
                                left=BinaryOp(
                                    left=Column(name="s_store_sk", quote_style=None),
                                    op=BinaryOpKind.Eq,
                                    right=Column(name="ctr_store_sk", quote_style=None),
                                ),
                                op=BinaryOpKind.And,
                                right=BinaryOp(
                                    left=Column(name="s_state", quote_style=None),
                                    op=BinaryOpKind.Eq,
                                    right=String(value="TN"),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="ctr_customer_sk", quote_style=None),
                                op=BinaryOpKind.Eq,
                                right=Column(name="c_customer_sk", quote_style=None),
                            ),
                        ),
                    ),
                ],
            ),
            group_by=[],
            having=None,
            projection=[Column(name="c_customer_id", quote_style=None)],
            where=BinaryOp(
                left=BinaryOp(
                    left=BinaryOp(
                        left=BinaryOp(
                            left=Column(name="ctr_total_return", quote_style=None),
                            op=BinaryOpKind.NotEq,
                            right=Query(
                                ctes=[],
                                select=Select(
                                    distinct=False,
                                    from_=From(
                                        table=Alias(
                                            name="ctr2",
                                            quote_style=None,
                                            child=Table(
                                                name="customer_total_return",
                                                quote_style=None,
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
                                                quote_style=None,
                                                args=[
                                                    Column(
                                                        name="ctr_total_return",
                                                        quote_style=None,
                                                    )
                                                ],
                                            ),
                                            op=BinaryOpKind.Multiply,
                                            right=Number(value="1.2"),
                                        )
                                    ],
                                    where=BinaryOp(
                                        left=Column(name="ctr_store_sk", quote_style=None),
                                        op=BinaryOpKind.Eq,
                                        right=Column(name="ctr_store_sk", quote_style=None),
                                    ),
                                    limit=None,
                                ),
                                subquery=True,
                            ),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(name="s_store_sk", quote_style=None),
                            op=BinaryOpKind.Eq,
                            right=Column(name="ctr_store_sk", quote_style=None),
                        ),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(name="s_state", quote_style=None),
                        op=BinaryOpKind.Eq,
                        right=String(value="TN"),
                    ),
                ),
                op=BinaryOpKind.And,
                right=BinaryOp(
                    left=Column(name="ctr_customer_sk", quote_style=None),
                    op=BinaryOpKind.Eq,
                    right=Column(name="c_customer_sk", quote_style=None),
                ),
            ),
            limit=Number(value="100"),
        ),
        subquery=False,
    )