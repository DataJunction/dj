"""
Custom types for annotations.
"""

# pylint: disable=missing-class-docstring

from __future__ import annotations

import re
from enum import Enum
from types import ModuleType
from typing import Any, Iterator, List, Literal, Optional, Tuple, TypedDict, Union

from sqlalchemy.types import Text, TypeDecorator
from typing_extensions import Protocol

from dj.errors import DJException


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


PRIMITIVE_TYPES = {
    "BYTES",
    "STR",
    "FLOAT",
    "INT",
    "DECIMAL",
    "BOOL",
    "DATETIME",
    "DATE",
    "TIME",
    "TIMEDELTA",
    "NULL",
    "WILDCARD",
}

COMPLEX_TYPES = {"ARRAY": 1, "MAP": 2}

TYPE_PATTERN = re.compile(r"(?P<outer>[A-Z]+)\[(?P<inner>.*?)\]$")


class ColumnTypeError(DJException):
    "Exception for bad column type"


class ColumnTypeMeta(type):
    """Metaclass for Columntype enabling `.` syntax for type access"""

    def __getattr__(cls, attr: str) -> "ColumnType":
        try:

            return ColumnType(attr)
        except ColumnTypeError:
            return type.__getattribute__(cls, attr)

    def __getitem__(cls, key) -> "ColumnType":
        return ColumnType(key)


# pylint: disable=C0301
class ColumnType(str, metaclass=ColumnTypeMeta):
    """
    Types for columns.

    These represent the values from the ``python_type`` attribute in SQLAlchemy columns.

    NOTE: `ColumnType` is just a special string type and can be used anywhere a string would be

        >>> ColumnType('Array[INT]')
        'ARRAY[INT]'

        >>> ColumnType['INT']
        'INT'

        >>> ColumnType.Array[ColumnType.Int]
        'ARRAY[INT]'

        >>> ColumnType.ARRAY[ColumnType.Int].args[0]
        'INT'

        >>> ColumnType.Map[ColumnType.INT, ColumnType.array[ColumnType.map[ColumnType.INT, ColumnType.array[ColumnType.STR]]]]
        'MAP[INT, ARRAY[MAP[INT, ARRAY[STR]]]]'

        >>> ColumnType.Map[ColumnType.str, ColumnType.Array[ColumnType.int]].args[1].args[0]
        'INT'
    """

    # pylint: enable=C0301
    args = None

    def __new__(cls, type_: str):

        if isinstance(type_, ColumnType):
            return type_
        type_ = type_.upper().strip()
        if type_ in COMPLEX_TYPES or type_ in PRIMITIVE_TYPES:
            validated = type_
        else:
            validated = cls._validate_type(type_)

        obj = str.__new__(cls, validated)
        return obj

    def __getitem__(self, keys) -> "ColumnType":
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        keys = tuple(keys)
        if self not in COMPLEX_TYPES:
            raise ColumnTypeError(f"The type {self} is not a complex type.")
        if len(keys) != COMPLEX_TYPES[self]:
            raise ColumnTypeError(
                f"{self} expects {COMPLEX_TYPES[self]} inner type(s) but got {len(keys)}.",
            )
        args = tuple(ColumnType(key) for key in keys)
        # need to add check if args are acceptable types for the generic
        obj = str.__new__(
            self.__class__,
            self + "[" + ", ".join(args) + "]",
        )
        obj.args = args
        return obj

    @classmethod
    def _validate_type(cls, type_: str):
        test = TYPE_PATTERN.match(type_)
        if test is not None:
            outer = test.group("outer")
            inner = test.group("inner")
            if outer not in COMPLEX_TYPES:
                raise ColumnTypeError(f"{outer} is not a KNOWN complex type.")
            inners = inner.split(",")
            return ColumnType(outer)[inners]
        if type_ not in PRIMITIVE_TYPES:
            raise ColumnTypeError(f"{type_} is not an acceptable type.")
        return ColumnType(type_)


# pylint: disable=W0223
class ColumnTypeDecorator(TypeDecorator):
    impl = Text

    def process_bind_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        return ColumnType(value)


# pylint: enable=W0223


class TypeEnum(str, Enum):
    """
    PEP 249 basic types.

    Unfortunately SQLAlchemy doesn't seem to offer an API for determining the types of the
    columns in a (SQL Core) query, and the DB API 2.0 cursor only offers very coarse
    types.
    """

    STRING = "STRING"
    BINARY = "BINARY"
    NUMBER = "NUMBER"
    DATETIME = "DATETIME"
    UNKNOWN = "UNKNOWN"


class QueryState(str, Enum):
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
