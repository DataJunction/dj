"""
Custom types for annotations.
"""

# pylint: disable=missing-class-docstring

from __future__ import annotations

import datetime
from types import ModuleType
from typing import Any, Iterator, List, Literal, Optional, Tuple, TypedDict, Union

from pydantic.datetime_parse import parse_datetime
from typing_extensions import Protocol

from datajunction_server.enum import StrEnum


class SQLADialect(Protocol):  # pylint: disable=too-few-public-methods
    """
    A SQLAlchemy dialect.
    """

    dbapi: ModuleType


# The ``type_code`` in a cursor description -- can really be anything
TypeCode = Any


# Cursor description
Description = Optional[
    List[
        Tuple[
            str,
            TypeCode,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[bool],
        ]
    ]
]


# A stream of data
Row = Tuple[Any, ...]
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

# sqloxide type hints
# Reference: https://github.com/sqlparser-rs/sqlparser-rs/blob/main/src/ast/query.rs


class Value(TypedDict, total=False):
    Number: Tuple[str, bool]
    SingleQuotedString: str
    Boolean: bool


class Limit(TypedDict):
    Value: Value


class Identifier(TypedDict):
    quote_style: Optional[str]
    value: str


class Bound(TypedDict, total=False):
    Following: int
    Preceding: int


class WindowFrame(TypedDict):
    end_bound: Bound
    start_bound: Bound
    units: str


class Expression(TypedDict, total=False):
    CompoundIdentifier: List["Identifier"]
    Identifier: Identifier
    Value: Value
    Function: Function  # type: ignore
    UnaryOp: UnaryOp  # type: ignore
    BinaryOp: BinaryOp  # type: ignore
    Case: Case  # type: ignore


class Case(TypedDict):
    conditions: List[Expression]
    else_result: Optional[Expression]
    operand: Optional[Expression]
    results: List[Expression]


class UnnamedArgument(TypedDict):
    Expr: Expression


class Argument(TypedDict, total=False):
    Unnamed: Union[UnnamedArgument, Wildcard]


class Over(TypedDict):
    order_by: List[Expression]
    partition_by: List[Expression]
    window_frame: WindowFrame


class Function(TypedDict):
    args: List[Argument]
    distinct: bool
    name: List[Identifier]
    over: Optional[Over]


class ExpressionWithAlias(TypedDict):
    alias: Identifier
    expr: Expression


class Offset(TypedDict):
    rows: str
    value: Expression


class OrderBy(TypedDict, total=False):
    asc: Optional[bool]
    expr: Expression
    nulls_first: Optional[bool]


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
    lateral_col_alias: List[Identifier]
    lateral_view: Expression
    lateral_view_name: List[Identifier]
    outer: bool


class TableAlias(TypedDict):
    columns: List[Identifier]
    name: Identifier


class Table(TypedDict):
    alias: Optional[TableAlias]
    args: List[Argument]
    name: List[Identifier]
    with_hints: List[Expression]


class Derived(TypedDict):
    lateral: bool
    subquery: "Body"  # type: ignore
    alias: Optional[TableAlias]


class Relation(TypedDict, total=False):
    Table: Table
    Derived: Derived


class JoinConstraint(TypedDict):
    On: Expression
    Using: List[Identifier]


class JoinOperator(TypedDict, total=False):
    Inner: JoinConstraint
    LeftOuter: JoinConstraint
    RightOuter: JoinConstraint
    FullOuter: JoinConstraint


CrossJoin = Literal["CrossJoin"]
CrossApply = Literal["CrossApply"]
OuterApply = Literal["Outerapply"]


class Join(TypedDict):
    join_operator: Union[JoinOperator, CrossJoin, CrossApply, OuterApply]
    relation: Relation


class From(TypedDict):
    joins: List[Join]
    relation: Relation


Select = TypedDict(
    "Select",
    {
        "cluster_by": List[Expression],
        "distinct": bool,
        "distribute_by": List[Expression],
        "from": List[From],
        "group_by": List[Expression],
        "having": Optional[BinaryOp],
        "lateral_views": List[LateralView],
        "projection": List[Union[Projection, Wildcard]],
        "selection": Optional[BinaryOp],
        "sort_by": List[Expression],
        "top": Optional[Top],
    },
)


class Body(TypedDict):
    Select: Select


CTETable = TypedDict(
    "CTETable",
    {
        "alias": TableAlias,
        "from": Optional[Identifier],
        "query": "Query",  # type: ignore
    },
)


class With(TypedDict):
    cte_tables: List[CTETable]


Query = TypedDict(
    "Query",
    {
        "body": Body,
        "fetch": Optional[Fetch],
        "limit": Optional[Limit],
        "lock": Optional[Literal["Share", "Update"]],
        "offset": Optional[Offset],
        "order_by": List[OrderBy],
        "with": Optional[With],
    },
)


# We could support more than just ``SELECT`` here.
class Statement(TypedDict):
    Query: Query


# A parse tree, result of ``sqloxide.parse_sql``.
ParseTree = List[Statement]  # type: ignore


class UTCDatetime(datetime.datetime):
    """
    A UTC extension of pydantic's normal datetime handling
    """

    @classmethod
    def __get_validators__(cls):
        """
        Extend the builtin pydantic datetime parser with a custom validate method
        """
        yield parse_datetime
        yield cls.validate

    @classmethod
    def validate(cls, value) -> str:
        """
        Convert to UTC
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)  # pragma: no cover

        return value.astimezone(datetime.timezone.utc)
