"""
Types to represent the DJ AST used as an intermediate representation for DJ operations
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from enum import Enum
from itertools import chain, zip_longest
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    Generator,
)

from typing_extensions import Self


def flatten(maybe_iterable: Any) -> Generator:
    """
    flattens `maybe_iterable` by descending into items that are of list, tuple, set, Iterator
    """
    if isinstance(maybe_iterable, Iterable):
        for subiterator in maybe_iterable:
            if not any(
                isinstance(subiterator, lts) for lts in (list, tuple, set, Iterator)
            ):
                yield subiterator
                continue
            for element in flatten(subiterator):
                yield element
    yield maybe_iterable


class Node(ABC):
    """
    Base class for all DJ AST nodes.

    DJ nodes are python dataclasses with the following patterns:
        - Attributes are either
            - primitives (int, float, str, bool, None)
            - iterable from (list, tuple, set)
            - Enum
            - descendant of `Node`
        - Attributes starting with '_' are "obfuscated" and are not included in `children`

    """

    _parents: Optional[Set["Node"]]

    @property
    def parents(self: Self) -> Set["Node"]:
        try:
            return self._parents
        except AttributeError:
            self._parents = set()
            return self._parents

    def add_parents(self: Self, *parents: "Node") -> Self:
        """
        add parents to the node
        """
        for parent in parents:
            self.parents.add(parent)
        return self

    def add_self_as_parent(self: Self) -> Self:
        """
        adds self as a parent to all children
        """
        for child in self.children:
            child.add_parents(self)
        return self

    def flatten(self: Self) -> Iterator["Node"]:
        """
        flatten the sub-ast of the node as an iterator
        """
        return self.filter(lambda node: True)

    def fields(
        self: Self,
        flat: bool = True,
        nodes_only: bool = True,
        obfuscated: bool = False,
        nones: bool = False,
    ) -> Iterator:
        """
        Args:
            flat: return a flattened iterator (if children are iterable)
            nodes_only: do not yield children that are not Nodes (trumped by `obfuscated`)
            obfuscated: yield fields that have leading underscores (typically accessed via a property)
            nones: yield values that are None (optional fields without a value); trumped by `nodes_only`

        Returns:
            Iterator: returns all children of a node given filters and optional flattening (by default Iterator[Node])
        """
        child_generator = (
            self.__dict__[field.name]
            for field in fields(self)
            if (not field.name.startswith("_") if not obfuscated else True)
        )  # exclude obfuscated fields if `obfuscated` is True
        if flat:
            child_generator = flatten(child_generator)

        if nodes_only:
            child_generator = filter(
                lambda child: isinstance(child, Node),
                child_generator,
            )

        if nones:
            child_generator = filter(lambda child: child is not None, child_generator)

        return child_generator

    @property
    def children(self: Self) -> Iterator["Node"]:
        """
        returns an iterator of all nodes that are one step from the current node down including through iterables
        """
        return self.fields(flat=True, nodes_only=True, obfuscated=False, nones=False)

    def filter(self: Self, func: Callable[["Node"], bool]) -> Iterator["Node"]:
        """
        find all nodes that `func` returns `True` for
        """
        if func(self):
            yield self
        for node in chain(*[child.filter(func) for child in self.children]):
            yield node

    def find_all(self: Self, node_type: Type["Node"]) -> Iterator["Node"]:
        """
        find all nodes of a particular type in the node's sub-ast
        """
        return self.filter(lambda n: isinstance(n, node_type))

    def apply(self: Self, func: Callable[["Node"], None]):
        """
        traverse ast and apply func to each Node
        """
        func(self)
        for child in self.children:
            child.apply(func)

    def compare(self: Self, other: "Node") -> bool:
        """
        compare two ASTs
        """
        return self == other and all(
            child.compare(other_child)
            for child, other_child in zip_longest(self.children, other.children)
        )

    def __eq__(self: Self, other: "Node") -> bool:
        """
        Compares two nodes for "top level" equality.
        Checks for type equality and primitive field types for full equality. Compares all others for type equality only. No recursing.
        Note: Does not check (sub)AST. See `Node.compare` for comparing (sub)ASTs.
        """
        primitives = {int, float, str, bool, type(None)}
        return type(self) == type(other) and all(
            s == o if type(s) in primitives else type(s) == type(o)
            for s, o in zip(
                (self.fields(False, False, False, True)),
                (other.fields(False, False, False, True)),
            )
        )

    @abstractmethod
    def __hash__(self: Self) -> int:
        """
        hash a node
        """


class Expression(Node):
    """an expression type simply for type checking"""


@dataclass
class Named(Expression):
    """An Expression that has a name"""

    name: str
    quote_style: Optional[str]

    @property
    def quoted_name(self: Self) -> str:
        return f'{self.quote_style if self.quote_style else ""}{self.name}{self.quote_style if self.quote_style else ""}'

    def alias_or_name(self: Self) -> str:
        if len(self.parents) == 1:
            parent = list(self.parents)[0]
            if isinstance(parent, Alias):
                return parent.name
        return self.name


class Operation(Expression):
    """a type to overarch types that operate on other expressions"""


@dataclass
class UnaryOp(Operation):
    op: str
    expr: Expression

    def __hash__(self: Self) -> int:
        return hash((UnaryOp, self.op))


class BinaryOpKind(Enum):
    Eq = "="
    Gt = ">"
    Lt = "<"
    GtEq = ">="
    LtEq = "<="
    BitwiseOr = "|"
    BitwiseAnd = "&"
    BitwiseXor = "^"
    Multiply = "*"
    Divide = "/"
    Plus = "+"
    Minus = "-"
    Modulo = "%"


@dataclass
class BinaryOp(Operation):
    left: Expression
    op: BinaryOpKind
    right: Expression

    def __hash__(self: Self) -> int:
        return hash((BinaryOp, self.op))


@dataclass
class Case(Expression):
    conditions: List[Expression]
    else_result: Optional[Expression]
    operand: Optional[Expression]
    results: List[Expression]

    def __hash__(self: Self) -> int:
        return id(self)


@dataclass
class Function(Named, Operation):
    args: List[Expression]

    def __hash__(self: Self) -> int:
        return hash(Function)


@dataclass
class Value(Expression):
    value: Union[str, bool, float, int]

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(self) == type(other) and self.value == other.value

    def __hash__(self: Self) -> int:
        return hash((self.__class__, self.value))


class Number(Value):  # Number
    value: Union[float, int]


class String(Value):  # SingleQuotedString
    value: str


class Boolean(Value):  # Boolean
    value: bool


NodeType = TypeVar("NodeType", bound=Node)


@dataclass
class Alias(Named, Generic[NodeType]):
    child: Node

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(other) == Alias and self.name == other.name

    def __hash__(self: Self) -> int:
        return hash((Alias, self.name))


@dataclass
class Column(Named):
    _table: Optional["Table"] = field(repr=False, default=None)

    @property
    def table(self: Self) -> "Table":
        return self._table

    def add_table(self: Self, table: "Table") -> Self:
        if self._table is None:
            self._tables = table
        return self

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(other) == Column and self.name == other.name

    def __hash__(self: Self) -> int:
        return hash((Column, self.name))


@dataclass
class Wildcard(Expression):
    _tables: List["Table"] = field(repr=False, default_factory=list)

    @property
    def tables(self) -> List["Table"]:
        return self._tables

    def add_tables(self, *tables: "Table") -> Self:
        for table in tables:
            self._tables.append(table)
        return self

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(other) == Wildcard

    def __hash__(self: Self) -> int:
        return id(Wildcard)


@dataclass
class Table(Named):
    _columns: List[Column] = field(repr=False, default_factory=list)

    @property
    def columns(self: Self) -> List[Column]:
        return self._columns

    def add_columns(self: Self, *columns: Column) -> Self:
        for column in columns:
            self._columns.append(column)
            column.add_table(self)
        return self

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(other) == Table and self.name == other.name

    def __hash__(self: Self) -> int:
        return hash((self.__class__, self.name))


class JoinKind(Enum):
    Inner = "INNER JOIN"
    LeftOuter = "LEFT JOIN"
    RightOuter = "RIGHT JOIN"
    FullOuter = "FULL JOIN"


@dataclass
class Join(Node):
    kind: JoinKind
    table: Union[Table, Alias]
    on: Expression

    def __hash__(self: Self) -> int:
        return hash((Join, self.kind))


@dataclass
class From(Node):
    table: Union[Table, Alias]
    joins: List[Join]

    def __hash__(self: Self) -> int:
        return id(self)


@dataclass
class Select(Node):
    distinct: bool
    from_: From
    group_by: List[Expression]
    having: Optional[Expression]
    projection: List[Union[Expression, Alias[Expression]]]
    where: Optional[Expression]
    limit: Optional[Number]

    # def __eq__(self: Self, other: Any) -> bool:
    #     return type(other) == Select

    def __hash__(self: Self) -> int:
        return id(self)


@dataclass
class Query(Node):
    ctes: List[Alias["Select"]]
    select: "Select"

    def __hash__(self):
        return id(self)

    # def add_self_as_parent(self) -> Self:
    #     for cte in self.ctes:
    #         cte.add_parents(self)
    #     self.select.add_parents(self)
    #     return self
