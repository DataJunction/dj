"""
Custom types for annotations.
"""

# pylint: disable=missing-class-docstring

from __future__ import annotations

import collections
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
    "TIMESTAMP",
    "DATE",
    "TIME",
    "TIMEDELTA",
    "NULL",
    "WILDCARD",
}

ALIASES = {
    "DATETIME": "TIMESTAMP",
    "DECIMAL": "FLOAT",
    "INTEGER": "INT",
    "STRING": "STR",
    "LIST": "ARRAY",
    "DICT": "MAP",
}


def process_array_args(*args: str) -> Tuple["ColumnType"]:
    """
    Validate the args of an ARRAY
    """
    if len(args) != 1:
        raise ColumnTypeError(
            f"{', '.join(args)} is not an acceptable type for ARRAY"
            if args
            else "ARRAY needs subtype defined.",
        )
    array_type = args[0]
    if isinstance(array_type, list) and array_type[0] in COMPLEX_TYPES:
        return (ColumnType(array_type[0])[array_type[1]],)
    return (ColumnType(args[0]),)


def process_map_args(*args: str) -> Tuple["ColumnType", "ColumnType"]:
    """
    Validate the args of an MAP
    """
    if len(args) < 2:
        raise ColumnTypeError(
            f"MAP expects 2 inner types but got {len(args)}.",
        )
    key_args, value_args = args[0], args[1]

    map_key_type = (
        ColumnType(key_args[0], "key")[key_args[1]]
        if isinstance(key_args, list)
        else ColumnType(key_args, "key")
    )
    if map_key_type not in PRIMITIVE_TYPES - {"WILDCARD", "NULL"}:
        raise ColumnTypeError(f"`{map_key_type}` is not an acceptable MAP key type.")

    map_value_type = (
        ColumnType(value_args[0], "value")[value_args[1]]
        if isinstance(value_args, list)
        else ColumnType(value_args, "value")
    )
    return map_key_type, map_value_type


def process_row_args(*args: str) -> Tuple["ColumnType", ...]:
    """
    Validate the args of a ROW
    """
    ret = []
    for arg in args:
        if not isinstance(arg, list):
            type_, name, *_ = (*arg.split(), None)
            ret.append(ColumnType(type_, name and name.strip("\"' ")))
        else:
            ret.append(ColumnType(arg[0])[arg[1]])
    return tuple(ret)


COMPLEX_TYPES = {
    "ARRAY": process_array_args,
    "MAP": process_map_args,
    "ROW": process_row_args,
}

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

        >>> ColumnType.Map['str', ColumnType.Array[ColumnType.int]].args[1].args[0]
        'INT'

        >>> ColumnType.Row[ColumnType.STR, "INT id", ColumnType.ARRAY[ColumnType.bytes]]
        'ROW[STR, INT, ARRAY[BYTES]]'

        >>> ColumnType.Row['int "number"'].args[0].name
        'number'

    """

    # pylint: enable=C0301
    args: Optional[Tuple[ColumnType, ...]] = None
    name: Optional[str] = None

    def __new__(cls, type_: str, name: Optional[str] = None):

        if isinstance(type_, ColumnType):
            return type_
        type_ = type_.upper().strip()
        if type_ in COMPLEX_TYPES or type_ in PRIMITIVE_TYPES:
            obj = str.__new__(cls, type_)
        elif type_ in ALIASES:
            obj = str.__new__(cls, ALIASES[type_])
        else:
            obj = cls._validate_type(type_)
        obj.name = name
        return obj

    def is_complex(self):
        """
        Method to check if the type is complex
        """
        return any(self.startswith(cmplx) for cmplx in COMPLEX_TYPES)

    @property
    def value(self) -> str:
        """
        Get the serialized value of the column type

        Validates the type can be serialized
        """
        if self.is_complex() and not self.args:
            raise ColumnTypeError(
                f"{self} cannot be serialized as it"
                " is a complex type not fully defined.",
            )

        return self

    def __getitem__(self, keys) -> "ColumnType":
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        keys = tuple(keys)
        if self not in COMPLEX_TYPES:
            raise ColumnTypeError(f"The type {self} is not a complex type.")

        args = COMPLEX_TYPES[self](*keys)
        # need to add check if args are acceptable types for the generic
        obj = str.__new__(
            self.__class__,
            self + "[" + ", ".join(arg for arg in args) + "]",
        )
        obj.args = args
        return obj

    @classmethod
    def _validate_type(cls, type_: str):
        nested = parse_nested_type(type_)[0]
        if isinstance(nested, list) and len(nested) > 1:
            if nested[0] not in COMPLEX_TYPES:
                raise ColumnTypeError(f"{nested[0]} is not a known complex type.")
            return ColumnType(nested[0])[nested[1]]
        type_ = ALIASES.get(type_, type_)
        if type_ not in PRIMITIVE_TYPES:
            raise ColumnTypeError(f"{type_} is not an acceptable type.")
        return ColumnType(type_)


def parse_nested_type(expr: str) -> List:
    """
    Parses a nested type expression.
    """
    tokenizer = re.compile(r"\s*([\[\]\,])\s*").split
    tokens = list(filter(None, tokenizer(expr)))

    stack = collections.deque()  # type: ignore
    top = items = []  # type: ignore

    for i, token in enumerate(tokens):
        next_token = tokens[i + 1] if i + 1 < len(tokens) else None
        if token == "[":
            stack.append(items)
            items.append([])
            items = items[-1]
        elif token == "]":
            if not stack:
                raise ColumnTypeError(f"Unbalanced parentheses: {expr}")
            stack.pop()
            items = stack.pop()
        elif token.upper() in COMPLEX_TYPES or next_token == "[":
            stack.append(items)
            items.append([token])
            items = items[-1]
        elif token != ",":
            items.append(token)
    if stack:
        raise ColumnTypeError("Missing type definition for complex type!")
    return top


# pylint: disable=W0223
class ColumnTypeDecorator(TypeDecorator):
    impl = Text

    def process_bind_param(self, value: ColumnType, dialect):
        return value.value

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
    TIMESTAMP = "TIMESTAMP"
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
