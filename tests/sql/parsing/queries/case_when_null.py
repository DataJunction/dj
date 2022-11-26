"""
fixtures for derived_subquery.sql
"""


import pytest

from dj.sql.parsing.ast import (
    Alias,
    Column,
    From,
    Query,
    Select,
    Table,
    Case,
    UnaryOp,
    UnaryOpKind,
    IsNull,
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
                    name="web",
                    quote_style="",
                    child=Table(name="web_v1", quote_style=""),
                ),
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[
                Alias(
                    name="item_sk",
                    quote_style="",
                    child=Case(
                        conditions=[
                            IsNull(expr=Column(name="item_sk", quote_style=""))
                        ],
                        else_result=Column(name="item_sk", quote_style=""),
                        operand=None,
                        results=[Column(name="item_sk", quote_style="")],
                    ),
                ),
                Alias(
                    name="d_date",
                    quote_style="",
                    child=Case(
                        conditions=[
                            UnaryOp(
                                op=UnaryOpKind.Not,
                                expr=IsNull(expr=Column(name="d_date", quote_style="")),
                            )
                        ],
                        else_result=Column(name="d_date", quote_style=""),
                        operand=None,
                        results=[Column(name="d_date", quote_style="")],
                    ),
                ),
                Alias(
                    name="web_sales",
                    quote_style="",
                    child=Column(name="cume_sales", quote_style=""),
                ),
                Alias(
                    name="store_sales",
                    quote_style="",
                    child=Column(name="cume_sales", quote_style=""),
                ),
            ],
            where=None,
            limit=None,
        ),
        ctes=[],
    ).compile_parents()
