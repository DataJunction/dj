"""
fixtures for tpcds_q99.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    Between,
    BinaryOp,
    BinaryOpKind,
    Case,
    Column,
    From,
    Function,
    Number,
    Query,
    Select,
    Table,
    UnaryOp,
    UnaryOpKind,
)


@pytest.fixture
def tpcds_q99():
    """
    dj ast for tpcds query 99
    """
    return Query(
        ctes=[],
        select=Select(
            distinct=False,
            from_=From(table=Table(name="catalog_sales", quote_style=None), joins=[]),
            group_by=[
                Function(
                    name="Substr",
                    quote_style=None,
                    args=[
                        Column(name="w_warehouse_name", quote_style=None),
                        Number(value=1),
                        Number(value=20),
                    ],
                ),
                Column(name="sm_type", quote_style=None),
                Column(name="cc_name", quote_style=None),
            ],
            having=BinaryOp(
                left=BinaryOp(
                    left=BinaryOp(
                        left=BinaryOp(
                            left=UnaryOp(
                                op=UnaryOpKind.Not,
                                expr=Between(
                                    expr=Column(name="d_month_seq", quote_style=None),
                                    low=UnaryOp(
                                        op=UnaryOpKind.Plus,
                                        expr=Number(value=1200),
                                    ),
                                    high=BinaryOp(
                                        left=Number(value=1200),
                                        op=BinaryOpKind.Plus,
                                        right=Number(value=11),
                                    ),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="cs_ship_date_sk", quote_style=None),
                                op=BinaryOpKind.Eq,
                                right=Column(name="d_date_sk", quote_style=None),
                            ),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(name="cs_warehouse_sk", quote_style=None),
                            op=BinaryOpKind.Eq,
                            right=Column(name="w_warehouse_sk", quote_style=None),
                        ),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(name="cs_ship_mode_sk", quote_style=None),
                        op=BinaryOpKind.Eq,
                        right=Column(name="sm_ship_mode_sk", quote_style=None),
                    ),
                ),
                op=BinaryOpKind.And,
                right=BinaryOp(
                    left=Column(name="cs_call_center_sk", quote_style=None),
                    op=BinaryOpKind.Eq,
                    right=Column(name="cc_call_center_sk", quote_style=None),
                ),
            ),
            projection=[
                Function(
                    name="Substr",
                    quote_style=None,
                    args=[
                        Column(name="w_warehouse_name", quote_style=None),
                        Number(value=1),
                        Number(value=20),
                    ],
                ),
                Column(name="sm_type", quote_style=None),
                Column(name="cc_name", quote_style=None),
                Alias(
                    name="30 days",
                    quote_style="'",
                    child=Function(
                        name="Sum",
                        quote_style=None,
                        args=[
                            Case(
                                conditions=[
                                    BinaryOp(
                                        left=BinaryOp(
                                            left=Column(
                                                name="cs_ship_date_sk",
                                                quote_style=None,
                                            ),
                                            op=BinaryOpKind.Minus,
                                            right=Column(
                                                name="cs_sold_date_sk",
                                                quote_style=None,
                                            ),
                                        ),
                                        op=BinaryOpKind.LtEq,
                                        right=Number(value=30),
                                    ),
                                ],
                                else_result=Number(value=0),
                                operand=None,
                                results=[Number(value=1)],
                            ),
                        ],
                    ),
                ),
                Alias(
                    name="31-60 days",
                    quote_style="'",
                    child=Function(
                        name="Sum",
                        quote_style=None,
                        args=[
                            Case(
                                conditions=[
                                    BinaryOp(
                                        left=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.Gt,
                                            right=Number(value=30),
                                        ),
                                        op=BinaryOpKind.And,
                                        right=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.LtEq,
                                            right=Number(value=60),
                                        ),
                                    ),
                                ],
                                else_result=Number(value=0),
                                operand=None,
                                results=[Number(value=1)],
                            ),
                        ],
                    ),
                ),
                Alias(
                    name="61-90 days",
                    quote_style="'",
                    child=Function(
                        name="Sum",
                        quote_style=None,
                        args=[
                            Case(
                                conditions=[
                                    BinaryOp(
                                        left=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.Gt,
                                            right=Number(value=60),
                                        ),
                                        op=BinaryOpKind.And,
                                        right=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.LtEq,
                                            right=Number(value=90),
                                        ),
                                    ),
                                ],
                                else_result=Number(value=0),
                                operand=None,
                                results=[Number(value=1)],
                            ),
                        ],
                    ),
                ),
                Alias(
                    name="91-120 days",
                    quote_style="'",
                    child=Function(
                        name="Sum",
                        quote_style=None,
                        args=[
                            Case(
                                conditions=[
                                    BinaryOp(
                                        left=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.Gt,
                                            right=Number(value=90),
                                        ),
                                        op=BinaryOpKind.And,
                                        right=BinaryOp(
                                            left=BinaryOp(
                                                left=Column(
                                                    name="cs_ship_date_sk",
                                                    quote_style=None,
                                                ),
                                                op=BinaryOpKind.Minus,
                                                right=Column(
                                                    name="cs_sold_date_sk",
                                                    quote_style=None,
                                                ),
                                            ),
                                            op=BinaryOpKind.LtEq,
                                            right=Number(value=120),
                                        ),
                                    ),
                                ],
                                else_result=Number(value=0),
                                operand=None,
                                results=[Number(value=1)],
                            ),
                        ],
                    ),
                ),
                Alias(
                    name=">120 days",
                    quote_style="'",
                    child=Function(
                        name="Sum",
                        quote_style=None,
                        args=[
                            Case(
                                conditions=[
                                    BinaryOp(
                                        left=BinaryOp(
                                            left=Column(
                                                name="cs_ship_date_sk",
                                                quote_style=None,
                                            ),
                                            op=BinaryOpKind.Minus,
                                            right=Column(
                                                name="cs_sold_date_sk",
                                                quote_style=None,
                                            ),
                                        ),
                                        op=BinaryOpKind.Gt,
                                        right=Number(value=120),
                                    ),
                                ],
                                else_result=Number(value=0),
                                operand=None,
                                results=[Number(value=1)],
                            ),
                        ],
                    ),
                ),
            ],
            where=BinaryOp(
                left=BinaryOp(
                    left=BinaryOp(
                        left=BinaryOp(
                            left=UnaryOp(
                                op=UnaryOpKind.Not,
                                expr=Between(
                                    expr=Column(name="d_month_seq", quote_style=None),
                                    low=UnaryOp(
                                        op=UnaryOpKind.Plus,
                                        expr=Number(value=1200),
                                    ),
                                    high=BinaryOp(
                                        left=Number(value=1200),
                                        op=BinaryOpKind.Plus,
                                        right=Number(value=11),
                                    ),
                                ),
                            ),
                            op=BinaryOpKind.And,
                            right=BinaryOp(
                                left=Column(name="cs_ship_date_sk", quote_style=None),
                                op=BinaryOpKind.Eq,
                                right=Column(name="d_date_sk", quote_style=None),
                            ),
                        ),
                        op=BinaryOpKind.And,
                        right=BinaryOp(
                            left=Column(name="cs_warehouse_sk", quote_style=None),
                            op=BinaryOpKind.Eq,
                            right=Column(name="w_warehouse_sk", quote_style=None),
                        ),
                    ),
                    op=BinaryOpKind.And,
                    right=BinaryOp(
                        left=Column(name="cs_ship_mode_sk", quote_style=None),
                        op=BinaryOpKind.Eq,
                        right=Column(name="sm_ship_mode_sk", quote_style=None),
                    ),
                ),
                op=BinaryOpKind.And,
                right=BinaryOp(
                    left=Column(name="cs_call_center_sk", quote_style=None),
                    op=BinaryOpKind.Eq,
                    right=Column(name="cc_call_center_sk", quote_style=None),
                ),
            ),
            limit=Number(value=100),
        ),
        subquery=False,
    )
