"""
Types to represent the DJ AST used as an intermediate representation for DJ operations
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from enum import Enum
from itertools import chain, zip_longest
from typing import (
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

from dj.utils import flatten


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
    def parents(self) -> Set["Node"]:
        """
        get the parents of the node
        """
        try:
            return self._parents  # type: ignore
        except AttributeError:
            self._parents = set()
            return self._parents

    def add_parents(self, *parents: "Node") -> "Node":
        """
        add parents to the node
        """
        for parent in parents:
            self.parents.add(parent)
        return self

    def add_self_as_parent(self) -> "Node":
        """
        adds self as a parent to all children
        """
        for child in self.children:
            child.add_parents(self)
        return self

    def flatten(self) -> Iterator["Node"]:
        """
        flatten the sub-ast of the node as an iterator
        """
        return self.filter(lambda _: True)

    def fields(
        self,
        flat: bool = True,
        nodes_only: bool = True,
        obfuscated: bool = False,
        nones: bool = False,
    ) -> Iterator:
        """
        Args:
            flat: return a flattened iterator (if children are iterable)
            nodes_only: do not yield children that are not Nodes (trumped by `obfuscated`)
            obfuscated: yield fields that have leading underscores
                (typically accessed via a property)
            nones: yield values that are None
                (optional fields without a value); trumped by `nodes_only`

        Returns:
            Iterator: returns all children of a node given filters
                and optional flattening (by default Iterator[Node])
        """
        child_generator = (
            self.__dict__[field.name]
            for field in fields(self)
            if (not field.name.startswith("_") if not obfuscated else True)
        )  # exclude obfuscated fields if `obfuscated` is True
        if flat:
            child_generator = flatten(child_generator)

        if nodes_only:
            child_generator = filter(  # type: ignore
                lambda child: isinstance(child, Node),
                child_generator,
            )

        if nones:
            child_generator = filter(lambda child: child is not None, child_generator)  # type: ignore # pylint: disable=C0301

        return child_generator

    @property
    def children(self) -> Iterator["Node"]:
        """
        returns an iterator of all nodes that are one step
        from the current node down including through iterables
        """
        return self.fields(flat=True, nodes_only=True, obfuscated=False, nones=False)

    def filter(self, func: Callable[["Node"], bool]) -> Iterator["Node"]:
        """
        find all nodes that `func` returns `True` for
        """
        if func(self):
            yield self
        for node in chain(*[child.filter(func) for child in self.children]):
            yield node

    def find_all(self, node_type: Type["Node"]) -> Iterator["Node"]:
        """
        find all nodes of a particular type in the node's sub-ast
        """
        return self.filter(lambda n: isinstance(n, node_type))

    def apply(self, func: Callable[["Node"], None]):
        """
        traverse ast and apply func to each Node
        """
        func(self)
        for child in self.children:
            child.apply(func)

    def compare(self, other: "Node") -> bool:
        """
        compare two ASTs
        """

        return self == other and all(
            child.compare(other_child)
            for child, other_child in zip_longest(self.children, other.children)
        )

    def __eq__(self, other) -> bool:
        """
        Compares two nodes for "top level" equality.
        Checks for type equality and primitive field types for full equality.
        Compares all others for type equality only. No recursing.
        Note: Does not check (sub)AST. See `Node.compare` for comparing (sub)ASTs.
        """
        primitives = {int, float, str, bool, type(None)}
        return type(self) == type(other) and all(  # pylint: disable=C0123
            s == o
            if type(s) in primitives  # pylint: disable=C0123
            else type(s) == type(o)  # pylint: disable=C0123
            for s, o in zip(
                (self.fields(False, False, False, True)),
                (other.fields(False, False, False, True)),
            )
        )

    @abstractmethod
    def __hash__(self) -> int:
        """
        hash a node
        """


class Expression(Node):
    """an expression type simply for type checking"""


@dataclass(eq=False)  # type: ignore
class Named(Expression):
    """An Expression that has a name"""

    name: str
    quote_style: Optional[str]

    @property
    def quoted_name(self) -> str:
        """
        get the name of the Named Node including the quotes if any
        """
        return f'{self.quote_style if self.quote_style else ""}{self.name}{self.quote_style if self.quote_style else ""}'  # pylint: disable=C0301

    def alias_or_name(self) -> str:
        """
        get the name or alias of the node
        """
        if len(self.parents) == 1:
            parent = list(self.parents)[0]
            if isinstance(parent, Alias):
                return parent.name
        return self.name


class Operation(Expression):
    """a type to overarch types that operate on other expressions"""


class UnaryOpKind(Enum):
    """the accepted unary operations"""

    Plus = "+"  # pylint: disable=C0103
    Minus = "-"  # pylint: disable=C0103
    Not = "NOT"  # pylint: disable=C0103


@dataclass(eq=False)
class UnaryOp(Operation):
    """an operation that operates on a single expression"""

    op: UnaryOpKind  # pylint: disable=C0103
    expr: Expression

    def __hash__(self) -> int:
        return hash((UnaryOp, self.op))


class BinaryOpKind(Enum):
    """the accepted binary operations"""

    And = "AND"  # pylint: disable=C0103
    Or = "OR"  # pylint: disable=C0103
    Eq = "="  # pylint: disable=C0103
    NotEq = "<>"  # pylint: disable=C0103
    Gt = ">"  # pylint: disable=C0103
    Lt = "<"  # pylint: disable=C0103
    GtEq = ">="  # pylint: disable=C0103
    LtEq = "<="  # pylint: disable=C0103
    BitwiseOr = "|"  # pylint: disable=C0103
    BitwiseAnd = "&"  # pylint: disable=C0103
    BitwiseXor = "^"  # pylint: disable=C0103
    Multiply = "*"  # pylint: disable=C0103
    Divide = "/"  # pylint: disable=C0103
    Plus = "+"  # pylint: disable=C0103
    Minus = "-"  # pylint: disable=C0103
    Modulo = "%"  # pylint: disable=C0103


@dataclass(eq=False)
class BinaryOp(Operation):
    """represents an operation that operates on two expressions"""

    left: Expression
    op: BinaryOpKind  # pylint: disable=C0103
    right: Expression

    def __hash__(self) -> int:
        return hash((BinaryOp, self.op))


@dataclass(eq=False)
class Between(Operation):
    """
    a between statement
    """

    expr: Expression
    low: Expression
    high: Expression

    def __hash__(self) -> int:
        return hash((Between, self.low, self.high))


@dataclass(eq=False)
class Case(Expression):
    """a case statement of branches"""

    conditions: List[Expression]
    else_result: Optional[Expression]
    operand: Optional[Expression]
    results: List[Expression]

    def __hash__(self) -> int:
        return id(self)


@dataclass(eq=False)
class Function(Named, Operation):
    """represents a function used in a statement"""

    args: List[Expression]

    def __hash__(self) -> int:
        return hash(Function)


@dataclass(eq=False)
class Value(Expression):
    """base class for all values number, string, boolean"""

    value: Union[str, bool, float, int]


@dataclass(eq=False)
class Number(Value):  # Number
    """number value"""

    value: Union[float, int]

    def __post_init__(self):
        if type(self.value) not in (float, int):
            try:
                self.value = int(self.value)
            except ValueError:
                self.value = float(self.value)

    def __hash__(self) -> int:
        return hash((Number, self.value))


class String(Value):  # SingleQuotedString
    """string value"""

    value: str

    def __hash__(self) -> int:
        return hash((String, self.value))


class Boolean(Value):  # Boolean
    """boolean True/False value"""

    value: bool

    def __hash__(self) -> int:
        return hash((Boolean, self.value))


NodeType = TypeVar("NodeType", bound=Node)  # pylint: disable=C0103


@dataclass(eq=False)
class Alias(Named, Generic[NodeType]):
    """wraps node types with an alias"""

    child: Node

    def __hash__(self) -> int:
        return hash((Alias, self.name))


@dataclass(eq=False)
class Column(Named):
    """column used in statements"""

    _table: Optional["Table"] = field(repr=False, default=None)

    @property
    def table(self) -> Optional["Table"]:
        """
        return the table the column was referenced from
        """
        return self._table

    def add_table(self, table: "Table") -> "Column":
        """
        add a referenced table
        """
        if self._table is None:
            self._table = table
        return self

    def __hash__(self) -> int:
        return hash((Column, self.name))


@dataclass(eq=False)
class Wildcard(Expression):
    """wildcard or '*' expression"""

    _table: Optional["Table"] = field(repr=False, default=None)

    @property
    def table(self) -> Optional["Table"]:
        """
        return the table the column was referenced from
        """
        return self._table

    def add_table(self, table: "Table") -> "Wildcard":
        """
        add a referenced table
        """
        if self._table is None:
            self._table = table
        return self

    def __hash__(self) -> int:
        return id(Wildcard)


@dataclass(eq=False)
class Table(Named):
    """a type for tables"""

    _columns: List[Column] = field(repr=False, default_factory=list)

    @property
    def columns(self) -> List[Column]:
        """
        return the columns referenced from this table
        """
        return self._columns

    def add_columns(self, *columns: Column) -> "Table":
        """
        add columns referenced from this table
        """
        for column in columns:
            self._columns.append(column)
            column.add_table(self)
        return self

    def __hash__(self) -> int:
        return hash((self.__class__, self.name))


class JoinKind(Enum):
    """the accepted kinds of joins"""

    Inner = "INNER JOIN"  # pylint: disable=C0103
    LeftOuter = "LEFT JOIN"  # pylint: disable=C0103
    RightOuter = "RIGHT JOIN"  # pylint: disable=C0103
    FullOuter = "FULL JOIN"  # pylint: disable=C0103


@dataclass(eq=False)
class Join(Node):
    """a join between tables"""

    kind: JoinKind
    table: Union[Table, Alias]
    on: Expression  # pylint: disable=C0103

    def __hash__(self) -> int:
        return hash((Join, self.kind))


@dataclass(eq=False)
class From(Node):
    """a from that belongs to a select"""

    table: Union[Table, Alias]
    joins: List[Join]

    def __hash__(self) -> int:
        return id(self)


@dataclass(eq=False)
class Select(Node):
    """a single select statement type"""

    distinct: bool
    from_: From
    group_by: List[Expression]
    having: Optional[Expression]
    projection: List[Union[Expression, Alias[Expression]]]
    where: Optional[Expression]
    limit: Optional[Number]

    def __hash__(self) -> int:
        return id(self)


@dataclass(eq=False)
class Query(Expression):
    """overarching query type"""

    ctes: List[Alias["Select"]]
    select: "Select"
    subquery: bool

    def __hash__(self):
        return id(self)
