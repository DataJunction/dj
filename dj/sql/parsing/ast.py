from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from enum import Enum
from itertools import chain
from typing import (
    Any,
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

import sqlalchemy
from typing_extensions import Self


def flatten(maybe_iterable):
    try:
        for subiterator in maybe_iterable:
            if isinstance(subiterator, str):
                yield subiterator
                continue
            for element in flatten(subiterator):
                yield element
    except TypeError:
        yield maybe_iterable


class Node(ABC):
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
        returns all children of a node given filters and optional flattening

        """
        child_generator = (
            self.__dict__[field.name]
            for field in fields(self)
            if not (
                obfuscated and field.name.startswith("_")
            )  # exclude obfuscated fields if `obfuscated` is True
        )
        if flat:
            child_generator = flatten(child_generator)

        if nodes_only:
            child_generator = filter(
                lambda child: isinstance(child, Node), child_generator,
            )

        if nones:
            child_generator = filter(lambda child: child is not None, child_generator)

        return child_generator

    @property
    def children(self: Self) -> Iterator["Node"]:
        """returns all nodes that are one step from the current node down including through iterables"""
        return self.fields(flat=True, nodes_only=True, obfuscated=False, nones=False)

    def filter(self: Self, func: Callable[["Node"], bool]) -> Iterator["Node"]:
        """
        find all nodes that `func` returns `True` for
        """
        if func(self):
            yield self
        for node in chain(
            *[
                child.filter(func)
                for child in self.fields(
                    flat=True, nodes_only=True, obfuscated=True, nones=False,
                )
            ]
        ):
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
            for child, other_child in zip(self.children, other.children)
        )

    def sql(self: Self) -> str:
        """
        return the ansi sql representing the sub-ast
        """
        return " ".join([child.sql() for child in self.children])

    def __eq__(self: Self, other: "Node") -> bool:
        """
        compares two nodes for equality
        Note: does not check sub ast only that node
        """
        return type(self) == type(other) and self.children == other.children

    @abstractmethod
    def __hash__(self: Self) -> int:
        """
        hash a node
        """

    @abstractmethod
    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        """
        takes a sqlalchemy select and binds to it whatever self represents
        """


class Expression(Node):
    """an expression type simply for type checking"""


class Operation(Expression):
    """a type to overarch types that operate on other expressions"""


@dataclass
class UnaryOp(Operation):
    op: str
    expr: Expression

    def __hash__(self: Self) -> int:
        return hash((UnaryOp, self.op))

    def sql(self) -> str:
        return f"{self.op} {self.expr.sql()}"

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


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

    def sql(self) -> str:
        return f"{self.left.sql()} {self.op.value} {self.right.sql()}"

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


@dataclass
class Value(Expression):
    value: Union[str, bool, float, int]

    def __eq__(self: Self, other: Any) -> bool:
        return type(self) == type(other) and self.value == other.value

    def __hash__(self: Self) -> int:
        return hash(self.value)

    def sql(self: Self) -> str:
        return str(self.value)


class Number(Value):  # Number
    value: Union[float, int]

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


class String(Value):  # SingleQuotedString
    value: str

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


class Boolean(Value):  # Boolean
    value: bool

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


@dataclass
class Named(Expression):
    """An Expression that has a name"""

    name: str
    quote_style: Optional[str]

    def sql(self: Self) -> str:
        return self.quoted_name

    @property
    def quoted_name(self: Self) -> str:
        return f'{self.quote_style if self.quote_style else ""}{self.name}{self.quote_style if self.quote_style else ""}'

    def alias_or_name(self: Self) -> str:
        if len(self.parents) == 1:
            parent = list(self.parents)[0]
            if isinstance(parent, Alias):
                return parent.name
        return self.name


NodeType = TypeVar("NodeType", bound=Node)


@dataclass
class Alias(Named, Generic[NodeType]):
    child: Node

    def sql(self: Self) -> str:
        return f"({self.child.sql()}) AS {self.quoted_name}"

    def __eq__(self: Self, other: Any) -> bool:
        return type(other) == Alias and self.name == other.name

    def __hash__(self: Self) -> int:
        return hash((self.__class__, self.name))

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        return self.child.sql_alchemy(select).alias(self.name)


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

    def __hash__(self: Self) -> int:
        return hash((self.__class__, self.name))

    def __eq__(self: Self, other: Any) -> bool:
        return type(other) == Column and self.name == other.name

    def sql(self: Self) -> str:
        if self.table:
            return f'{self.quote_style if self.quote_style else ""}{self.table.alias_or_name()}.{self.name}{self.quote_style if self.quote_style else ""}'

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


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

    def sql(self: Self) -> str:
        return "*"

    def __eq__(self: Self, other: Any) -> bool:
        return type(other) == Wildcard

    def __hash__(self: Self) -> int:
        return id(Wildcard)

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


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

    def __hash__(self: Self) -> int:
        return hash((self.__class__, self.name))

    def __eq__(self: Self, other: Any) -> bool:
        return type(other) == Table and self.name == other.name

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


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
        return hash((From, self.kind))

    def sql(self: Self) -> str:
        return f"""{self.kind.value} {self.table.sql()}\n\tON {self.on.sql()}"""

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


@dataclass
class From(Node):
    table: Union[Table, Alias]
    joins: List[Join]

    def __hash__(self: Self) -> int:
        return id(From)

    def sql(self: Self) -> str:
        return (
            f"FROM {self.table.sql()}"
            + "\n"
            + "\n".join([join.sql() for join in self.joins])
        )

    def sql_alchemy(self: Self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


@dataclass
class Select(Node):
    distinct: bool
    from_: From
    group_by: List[Expression]
    having: Optional[Expression]
    projection: List[Union[Expression, Alias[Expression]]]
    where: Optional[Expression]
    #     sort_by: List[Expression]
    #     limit: Optional[Number] #not in ansi sql

    def __eq__(self: Self, other: Any) -> bool:
        return type(other) == Select

    def __hash__(self: Self) -> int:
        return id(self)

    def sql(self: Self) -> str:
        projection = ",\n\t".join(exp.sql() for exp in self.projection)
        return f"""SELECT {"DISTINCT " if self.distinct else ""}{projection}
{self.from_.sql()}
{"WHERE "+self.where.sql() if self.where is not None else ""}
{"GROUP BY "+", ".join([exp.sql() for exp in self.group_by]) if self.group_by else ""}
{"HAVING "+self.having.sql() if self.having is not None else ""}
""".strip()

    def sql_alchemy(self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        raise NotImplementedError()


@dataclass
class Query(Node):
    ctes: List[Alias["Select"]]
    select: "Select"

    def __hash__(self):
        return id(self)

    def add_self_as_parent(self) -> Self:
        for cte in self.ctes:
            cte.add_parents(self)
        self.select.add_parents(self)
        return self

    def sql(self: Self) -> str:
        ctes = ",\n".join(cte.sql() for cte in self.ctes)
        return f"""{'WITH' if ctes else ""}
{ctes}

{self.select.sql()}
        """.strip()

    def sql_alchemy(self, select: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        return self.child.sql_alchemy(select).alias(self.name)


def parse_query(parse_tree) -> Query:
    return Query(
        parse_ctes(parse_tree["with"]) if parse_tree["with"] is not None else [],
        parse_select(parse_tree["body"]["Select"]),
    ).add_self_as_parent()

    raise Exception("Failed to parse query")
