"""
fixtures for derived_subquery.sql
"""


import pytest

from dj.sql.parsing import ast


@pytest.fixture
def case_when_null():
    """
    dj ast for case when null
    """
    return ast.Query(
        select=ast.Select(
            from_=ast.From(
                tables=[
                    ast.Alias(
                        name=ast.Name(name="web", quote_style=""),
                        namespace=None,
                        child=ast.Table(
                            name=ast.Name(name="web_v1", quote_style=""),
                            namespace=None,
                        ),
                    ),
                ],
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[
                ast.Alias(
                    name=ast.Name(name="item_sk", quote_style=""),
                    namespace=None,
                    child=ast.Case(
                        conditions=[
                            ast.IsNull(
                                expr=ast.Column(
                                    name=ast.Name(name="item_sk", quote_style=""),
                                    namespace=ast.Namespace(
                                        names=[ast.Name(name="web", quote_style="")],
                                    ),
                                ),
                            ),
                        ],
                        else_result=ast.Column(
                            name=ast.Name(name="item_sk", quote_style=""),
                            namespace=ast.Namespace(
                                names=[ast.Name(name="store", quote_style="")],
                            ),
                        ),
                        operand=None,
                        results=[
                            ast.Column(
                                name=ast.Name(name="item_sk", quote_style=""),
                                namespace=ast.Namespace(
                                    names=[ast.Name(name="web", quote_style="")],
                                ),
                            ),
                        ],
                    ),
                ),
                ast.Alias(
                    name=ast.Name(name="d_date", quote_style=""),
                    namespace=None,
                    child=ast.Case(
                        conditions=[
                            ast.UnaryOp(
                                op=ast.UnaryOpKind.Not,
                                expr=ast.IsNull(
                                    expr=ast.Column(
                                        name=ast.Name(name="d_date", quote_style=""),
                                        namespace=ast.Namespace(
                                            names=[
                                                ast.Name(name="web", quote_style=""),
                                            ],
                                        ),
                                    ),
                                ),
                            ),
                        ],
                        else_result=ast.Column(
                            name=ast.Name(name="d_date", quote_style=""),
                            namespace=ast.Namespace(
                                names=[ast.Name(name="store", quote_style="")],
                            ),
                        ),
                        operand=None,
                        results=[
                            ast.Column(
                                name=ast.Name(name="d_date", quote_style=""),
                                namespace=ast.Namespace(
                                    names=[ast.Name(name="web", quote_style="")],
                                ),
                            ),
                        ],
                    ),
                ),
                ast.Alias(
                    name=ast.Name(name="web_sales", quote_style=""),
                    namespace=None,
                    child=ast.Column(
                        name=ast.Name(name="cume_sales", quote_style=""),
                        namespace=ast.Namespace(
                            names=[ast.Name(name="web", quote_style="")],
                        ),
                    ),
                ),
                ast.Alias(
                    name=ast.Name(name="store_sales", quote_style=""),
                    namespace=None,
                    child=ast.Column(
                        name=ast.Name(name="cume_sales", quote_style=""),
                        namespace=ast.Namespace(
                            names=[ast.Name(name="store", quote_style="")],
                        ),
                    ),
                ),
            ],
            where=None,
            limit=None,
            distinct=False,
        ),
        ctes=[],
        dialect="ansi",
    )
