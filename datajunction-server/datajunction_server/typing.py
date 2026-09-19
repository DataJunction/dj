"""
Custom types for annotations.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterator
from types import ModuleType
from typing import Any, Literal, Optional, TypedDict

from typing_extensions import Protocol

from datajunction_server.enum import StrEnum


class SQLADialect(Protocol):
    """
    A SQLAlchemy dialect.
    """

    dbapi: ModuleType


# The ``type_code`` in a cursor description -- can really be anything
TypeCode = Any


# Cursor description
Description = Optional[
    list[
        tuple[
            str,
            TypeCode,
            str | None,
            str | None,
            str | None,
            str | None,
            bool | None,
        ]
    ]
]


# A stream of data
Row = tuple[Any, ...]
Stream = Iterator[Row]


class TypeEnum(StrEnum):
    """
    PEP 249 basic types.

    Unfortunately SQLAlchemy doesn't seem to offer an API for determining the types of the
    columns in a (SQL Core) query, and the DB API 2.0 cursor only offers very coarse
    types.
    """

    STRING = "STRING"
    BINARY = "BINARY"
    NUMBER = "NUMBER"
    TIMESTAMP = "TIMESTAMP"
    UNKNOWN = "UNKNOWN"


class QueryState(StrEnum):
    """
    Different states of a query.
    """

    UNKNOWN = "UNKNOWN"
    ACCEPTED = "ACCEPTED"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


END_JOB_STATES = [QueryState.FINISHED, QueryState.CANCELED, QueryState.FAILED]


class Value(TypedDict, total=False):
    Number: tuple[str, bool]
    SingleQuotedString: str
    Boolean: bool


class Limit(TypedDict):
    Value: Value


class Identifier(TypedDict):
    quote_style: str | None
    value: str


class Bound(TypedDict, total=False):
    Following: int
    Preceding: int


class WindowFrame(TypedDict):
    end_bound: Bound
    start_bound: Bound
    units: str


class Expression(TypedDict, total=False):
    CompoundIdentifier: list[Identifier]
    Identifier: Identifier
    Value: Value
    Function: Function  # type: ignore
    UnaryOp: UnaryOp  # type: ignore
    BinaryOp: BinaryOp  # type: ignore
    Case: Case  # type: ignore


class Case(TypedDict):
    conditions: list[Expression]
    else_result: Expression | None
    operand: Expression | None
    results: list[Expression]


class UnnamedArgument(TypedDict):
    Expr: Expression


class Argument(TypedDict, total=False):
    Unnamed: UnnamedArgument | Wildcard


class Over(TypedDict):
    order_by: list[Expression]
    partition_by: list[Expression]
    window_frame: WindowFrame


class Function(TypedDict):
    args: list[Argument]
    distinct: bool
    name: list[Identifier]
    over: Over | None


class ExpressionWithAlias(TypedDict):
    alias: Identifier
    expr: Expression


class Offset(TypedDict):
    rows: str
    value: Expression


class OrderBy(TypedDict, total=False):
    asc: bool | None
    expr: Expression
    nulls_first: bool | None


class Projection(TypedDict, total=False):
    ExprWithAlias: ExpressionWithAlias
    UnnamedExpr: Expression


Wildcard = Literal["Wildcard"]


class Fetch(TypedDict):
    percent: bool
    quantity: Value
    with_ties: bool


Top = Fetch


class UnaryOp(TypedDict):
    op: str
    expr: Expression


class BinaryOp(TypedDict):
    left: Expression
    op: str
    right: Expression


class LateralView(TypedDict):
    lateral_col_alias: list[Identifier]
    lateral_view: Expression
    lateral_view_name: list[Identifier]
    outer: bool


class TableAlias(TypedDict):
    columns: list[Identifier]
    name: Identifier


class Table(TypedDict):
    alias: TableAlias | None
    args: list[Argument]
    name: list[Identifier]
    with_hints: list[Expression]


class Derived(TypedDict):
    lateral: bool
    subquery: Body  # type: ignore
    alias: TableAlias | None


class Relation(TypedDict, total=False):
    Table: Table
    Derived: Derived


class JoinConstraint(TypedDict):
    On: Expression
    Using: list[Identifier]


class JoinOperator(TypedDict, total=False):
    Inner: JoinConstraint
    LeftOuter: JoinConstraint
    RightOuter: JoinConstraint
    FullOuter: JoinConstraint


CrossJoin = Literal["CrossJoin"]
CrossApply = Literal["CrossApply"]
OuterApply = Literal["Outerapply"]


class Join(TypedDict):
    join_operator: JoinOperator | CrossJoin | CrossApply | OuterApply
    relation: Relation


class From(TypedDict):
    joins: list[Join]
    relation: Relation


Select = TypedDict(
    "Select",
    {
        "cluster_by": list[Expression],
        "distinct": bool,
        "distribute_by": list[Expression],
        "from": list[From],
        "group_by": list[Expression],
        "having": BinaryOp | None,
        "lateral_views": list[LateralView],
        "projection": list[Projection | Wildcard],
        "selection": BinaryOp | None,
        "sort_by": list[Expression],
        "top": Top | None,
    },
)


class Body(TypedDict):
    Select: Select


CTETable = TypedDict(
    "CTETable",
    {
        "alias": TableAlias,
        "from": Identifier | None,
        "query": "Query",  # type: ignore
    },
)


class With(TypedDict):
    cte_tables: list[CTETable]


Query = TypedDict(
    "Query",
    {
        "body": Body,
        "fetch": Fetch | None,
        "limit": Limit | None,
        "lock": Literal["Share", "Update"] | None,
        "offset": Offset | None,
        "order_by": list[OrderBy],
        "with": With | None,
    },
)


# We could support more than just ``SELECT`` here.
class Statement(TypedDict):
    Query: Query


ParseTree = list[Statement]  # type: ignore


class UTCDatetime(datetime.datetime):
    """
    A UTC extension of pydantic's normal datetime handling
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type,
        handler,
    ):
        """
        Pydantic v2 core schema for datetime validation
        """
        from pydantic_core import core_schema

        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.datetime_schema(),
        )

    @classmethod
    def validate(cls, value):
        """
        Convert to UTC
        """
        if isinstance(value, str):  # pragma: no cover
            # Parse string to datetime first
            from datetime import datetime as dt

            try:
                value = dt.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                # Try other common formats
                import dateutil.parser

                value = dateutil.parser.parse(value)

        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.UTC)  # pragma: no cover

        return value.astimezone(datetime.UTC)
