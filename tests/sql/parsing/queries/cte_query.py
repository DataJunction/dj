"""a query with a cte"""

import pytest

from dj.sql.parsing.ast import (
    Alias,
    BinaryOp,
    BinaryOpKind,
    Column,
    From,
    IsNull,
    Name,
    Namespace,
    Query,
    Select,
    String,
    Table,
)


@pytest.fixture
def cte_query():
    """
    a cte query
    """
    return Query(
        select=Select(
            from_=From(
                tables=[
                    Table(name=Name(name="cteReports", quote_style=""), namespace=None),
                ],
                joins=[],
            ),
            group_by=[],
            having=None,
            projection=[
                Alias(
                    name=Name(name="FullName", quote_style=""),
                    namespace=None,
                    child=BinaryOp(
                        op=BinaryOpKind.Plus,
                        left=BinaryOp(
                            op=BinaryOpKind.Plus,
                            left=Column(
                                name=Name(name="FirstName", quote_style=""),
                                namespace=None,
                            ),
                            right=String(value=" "),
                        ),
                        right=Column(
                            name=Name(name="LastName", quote_style=""),
                            namespace=None,
                        ),
                    ),
                ),
                Column(name=Name(name="EmpLevel", quote_style=""), namespace=None),
                Alias(
                    name=Name(name="Manager", quote_style=""),
                    namespace=None,
                    child=Query(
                        select=Select(
                            from_=From(
                                tables=[
                                    Table(
                                        name=Name(name="Employees", quote_style=""),
                                        namespace=None,
                                    ),
                                ],
                                joins=[],
                            ),
                            group_by=[],
                            having=None,
                            projection=[
                                BinaryOp(
                                    op=BinaryOpKind.Plus,
                                    left=BinaryOp(
                                        op=BinaryOpKind.Plus,
                                        left=Column(
                                            name=Name(name="FirstName", quote_style=""),
                                            namespace=None,
                                        ),
                                        right=String(value=" "),
                                    ),
                                    right=Column(
                                        name=Name(name="LastName", quote_style=""),
                                        namespace=None,
                                    ),
                                ),
                            ],
                            where=BinaryOp(
                                op=BinaryOpKind.Eq,
                                left=Column(
                                    name=Name(name="EmployeeID", quote_style=""),
                                    namespace=None,
                                ),
                                right=Column(
                                    name=Name(name="MgrID", quote_style=""),
                                    namespace=Namespace(
                                        names=[Name(name="cteReports", quote_style="")],
                                    ),
                                ),
                            ),
                            limit=None,
                            distinct=False,
                        ),
                        ctes=[],
                    ),
                ),
            ],
            where=None,
            limit=None,
            distinct=False,
        ),
        ctes=[
            Alias(
                name=Name(name="cteReports", quote_style=""),
                namespace=None,
                child=Select(
                    from_=From(
                        tables=[
                            Table(
                                name=Name(name="Employees", quote_style=""),
                                namespace=None,
                            ),
                        ],
                        joins=[],
                    ),
                    group_by=[],
                    having=None,
                    projection=[
                        Column(
                            name=Name(name="EmployeeID", quote_style=""),
                            namespace=None,
                        ),
                        Column(
                            name=Name(name="FirstName", quote_style=""),
                            namespace=None,
                        ),
                        Column(
                            name=Name(name="LastName", quote_style=""),
                            namespace=None,
                        ),
                        Column(
                            name=Name(name="ManagerID", quote_style=""),
                            namespace=None,
                        ),
                    ],
                    where=IsNull(
                        expr=Column(
                            name=Name(name="ManagerID", quote_style=""),
                            namespace=None,
                        ),
                    ),
                    limit=None,
                    distinct=False,
                ),
            ),
        ],
    )
