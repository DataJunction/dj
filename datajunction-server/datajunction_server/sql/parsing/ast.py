# pylint: disable=R0401,C0302
# pylint: skip-file
# mypy: ignore-errors
import collections
import decimal
import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field, fields
from enum import Enum
from functools import reduce
from itertools import chain, zip_longest
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.construction.utils import get_dj_node, to_namespaced_name
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node as DJNodeRef
from datajunction_server.database.node import NodeRevision
from datajunction_server.database.node import NodeRevision as DJNode
from datajunction_server.errors import DJError, DJErrorException, DJException, ErrorCode
from datajunction_server.models.column import SemanticType
from datajunction_server.models.node import BuildCriteria
from datajunction_server.models.node_type import NodeType as DJNodeType
from datajunction_server.sql.functions import function_registry, table_function_registry
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.types import (
    BigIntType,
    BooleanType,
    ColumnType,
    DateTimeBase,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerBase,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    NullType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    WildcardType,
    YearMonthIntervalType,
)
from datajunction_server.utils import SEPARATOR

PRIMITIVES = {int, float, str, bool, type(None)}
logger = logging.getLogger(__name__)


def flatten(maybe_iterables: Any) -> Iterator:
    """
    Flattens `maybe_iterables` by descending into items that are Iterable
    """

    if not isinstance(maybe_iterables, (list, tuple, set, Iterator)):
        return iter([maybe_iterables])
    return chain.from_iterable(
        (flatten(maybe_iterable) for maybe_iterable in maybe_iterables)
    )


@dataclass
class CompileContext:
    session: AsyncSession
    exception: DJException


# typevar used for node methods that return self
# so the typesystem can correlate the self type with the return type
TNode = TypeVar("TNode", bound="Node")  # pylint: disable=C0103


class Node(ABC):
    """Base class for all DJ AST nodes.

    DJ nodes are python dataclasses with the following patterns:
        - Attributes are either
            - PRIMITIVES (int, float, str, bool, None)
            - iterable from (list, tuple, set)
            - Enum
            - descendant of `Node`
        - Attributes starting with '_' are "obfuscated" and are not included in `children`

    """

    parent: Optional["Node"] = None
    parent_key: Optional[str] = None

    _is_compiled: bool = False

    def __post_init__(self):
        self.add_self_as_parent()

    @property
    def depth(self) -> int:
        if self.parent is None:
            return 0
        return self.parent.depth + 1

    def clear_parent(self: TNode) -> TNode:
        """
        Remove parent from the node
        """
        self.parent = None
        return self

    def set_parent(self: TNode, parent: "Node", parent_key: str) -> TNode:
        """
        Add parent to the node
        """
        self.parent = parent
        self.parent_key = parent_key
        return self

    def add_self_as_parent(self: TNode) -> TNode:
        """
        Adds self as a parent to all children
        """
        for name, child in self.fields(
            flat=True,
            nodes_only=True,
            obfuscated=False,
            nones=False,
            named=True,
        ):
            child.set_parent(self, name)
        return self

    def __setattr__(self, key: str, value: Any):
        """
        Facilitates setting children using `.` syntax ensuring parent is attributed
        """
        if key == "parent":
            object.__setattr__(self, key, value)
            return

        object.__setattr__(self, key, value)
        for child in flatten(value):
            if isinstance(child, Node) and not key.startswith("_"):
                child.set_parent(self, key)

    def swap(self: TNode, other: "Node") -> TNode:
        """
        Swap the Node for another
        """
        if not (self.parent and self.parent_key):
            return self
        parent_attr = getattr(self.parent, self.parent_key)
        if parent_attr is self:
            setattr(self.parent, self.parent_key, other)
            return self.clear_parent()

        new = []
        for iterable_type in (list, tuple, set):
            if isinstance(parent_attr, iterable_type):
                for element in parent_attr:
                    if self is element:
                        new.append(other)
                    else:
                        new.append(element)
                new = iterable_type(new)
                break

        setattr(self.parent, self.parent_key, new)
        return self.clear_parent()

    def copy(self: TNode) -> TNode:
        """
        Create a deep copy of the `self`
        """
        return deepcopy(self)

    def get_nearest_parent_of_type(
        self: "Node",
        node_type: Type[TNode],
    ) -> Optional[TNode]:
        """
        Traverse up the tree until you find a node of `node_type` or hit the root
        """
        if isinstance(self.parent, node_type):
            return self.parent
        if self.parent is None:
            return None
        return self.parent.get_nearest_parent_of_type(node_type)

    def get_furthest_parent(
        self: "Node",
    ) -> Optional[TNode]:
        """
        Traverse up the tree until you find a node of `node_type` or hit the root
        """
        if self.parent is None:
            return None
        curr_parent = self.parent
        while True:
            if curr_parent.parent is None:
                return curr_parent
            curr_parent = curr_parent.parent

    def flatten(self) -> Iterator["Node"]:
        """
        Flatten the sub-ast of the node as an iterator
        """
        return self.filter(lambda _: True)

    # pylint: disable=R0913
    def fields(
        self,
        flat: bool = True,
        nodes_only: bool = True,
        obfuscated: bool = False,
        nones: bool = False,
        named: bool = False,
    ) -> Iterator:
        """
        Returns an iterator over fields of a node with particular filters

        Args:
            flat: return a flattened iterator (if children are iterable)
            nodes_only: do not yield children that are not Nodes (trumped by `obfuscated`)
            obfuscated: yield fields that have leading underscores
                (typically accessed via a property)
            nones: yield values that are None
                (optional fields without a value); trumped by `nodes_only`
            named: yield pairs `(field name: str, field value)`
        Returns:
            Iterator: returns all children of a node given filters
                and optional flattening (by default Iterator[Node])
        """

        def make_child_generator():
            """
            Makes a generator enclosing self to return
            not obfuscated fields (fields without starting `_`)
            """
            for self_field in fields(self):
                if (
                    not self_field.name.startswith("_") if not obfuscated else True
                ) and (self_field.name in self.__dict__):
                    value = self.__dict__[self_field.name]
                    values = [value]
                    if flat:
                        values = flatten(value)
                    for value in values:
                        if named:
                            yield (self_field.name, value)
                        else:
                            yield value

        # `iter`s used to satisfy mypy (`child_generator` type changes between generator, filter)
        child_generator = iter(make_child_generator())

        if nodes_only:
            child_generator = iter(
                filter(
                    lambda child: isinstance(child, Node)
                    if not named
                    else isinstance(child[1], Node),
                    child_generator,
                ),
            )

        if not nones:
            child_generator = iter(
                filter(
                    lambda child: (child is not None)
                    if not named
                    else (child[1] is not None),
                    child_generator,
                ),
            )  # pylint: disable=C0301

        return child_generator

    @property
    def children(self) -> Iterator["Node"]:
        """
        Returns an iterator of all nodes that are one
        step from the current node down including through iterables
        """
        return self.fields(
            flat=True,
            nodes_only=True,
            obfuscated=False,
            nones=False,
            named=False,
        )

    def replace(  # pylint: disable=invalid-name
        self,
        from_: "Node",
        to: "Node",
        compare: Optional[Callable[[Any, Any], bool]] = None,
        times: int = -1,
        copy: bool = True,
    ):
        """
        Replace a node `from_` with a node `to` in the subtree
        """
        replacements = 0
        compare_ = (lambda a, b: a is b) if compare is None else compare
        for node in self.flatten():
            if compare_(node, from_):
                other = to.copy() if copy else to
                if isinstance(from_, Table) and from_.parent == to:
                    continue
                if isinstance(from_, Table):
                    for ref in from_.ref_columns:
                        ref.add_table(other)
                node.swap(other)
                replacements += 1
            if replacements == times:
                return

    def filter(self, func: Callable[["Node"], bool]) -> Iterator["Node"]:
        """
        Find all nodes that `func` returns `True` for
        """
        if func(self):
            yield self

        for node in chain(*[child.filter(func) for child in self.children]):
            yield node

    def contains(self, other: "Node") -> bool:
        """
        Checks if the subtree of `self` contains the node
        """
        return any(self.filter(lambda node: node is other))

    def is_ancestor_of(self, other: Optional["Node"]) -> bool:
        """
        Checks if `self` is an ancestor of the node
        """
        return bool(other) and other.contains(self)

    def find_all(self, node_type: Type[TNode]) -> Iterator[TNode]:
        """
        Find all nodes of a particular type in the node's sub-ast
        """
        return self.filter(lambda n: isinstance(n, node_type))  # type: ignore

    def apply(self, func: Callable[["Node"], None]):
        """
        Traverse ast and apply func to each Node
        """
        func(self)
        for child in self.children:
            child.apply(func)

    def compare(
        self,
        other: "Node",
    ) -> bool:
        """
        Compare two ASTs for deep equality
        """
        if type(self) != type(other):  # pylint: disable=unidiomatic-typecheck
            return False
        if id(self) == id(other):
            return True
        return hash(self) == hash(other)

    def diff(self, other: "Node") -> List[Tuple["Node", "Node"]]:
        """
        Compare two ASTs for differences and return the pairs of differences
        """

        def _diff(self, other: "Node"):
            if self != other:
                diffs.append((self, other))
            else:
                for child, other_child in zip_longest(self.children, other.children):
                    _diff(child, other_child)

        diffs: List[Tuple["Node", "Node"]] = []
        _diff(self, other)
        return diffs

    def similarity_score(self, other: "Node") -> float:
        """
        Determine how similar two nodes are with a float score
        """
        self_nodes = list(self.flatten())
        other_nodes = list(other.flatten())
        intersection = [
            self_node for self_node in self_nodes if self_node in other_nodes
        ]
        union = (
            [self_node for self_node in self_nodes if self_node not in intersection]
            + [
                other_node
                for other_node in other_nodes
                if other_node not in intersection
            ]
            + intersection
        )
        return len(intersection) / len(union)

    def __eq__(self, other) -> bool:
        """
        Compares two nodes for "top level" equality.

        Checks for type equality and primitive field types for full equality.
        Compares all others for type equality only. No recursing.
        Note: Does not check (sub)AST. See `Node.compare` for comparing (sub)ASTs.
        """
        return type(self) == type(other) and all(  # pylint: disable=C0123
            s == o
            if type(s) in PRIMITIVES  # pylint: disable=C0123
            else type(s) == type(o)  # pylint: disable=C0123
            for s, o in zip(
                (self.fields(False, False, False, True)),
                (other.fields(False, False, False, True)),
            )
        )

    def __hash__(self) -> int:
        """
        Hash a node
        """
        return hash(
            tuple(
                chain(
                    (type(self),),
                    self.fields(
                        flat=True,
                        nodes_only=False,
                        obfuscated=False,
                        nones=True,
                        named=False,
                    ),
                ),
            ),
        )

    @abstractmethod
    def __str__(self) -> str:
        """
        Get the string of a node
        """

    async def compile(self, ctx: CompileContext):
        """
        Compile a DJ Node. By default, we call compile on all immediate children of this node.
        """
        if self._is_compiled:
            return
        for child in self.children:
            if not child.is_compiled():
                await child.compile(ctx)
                child._is_compiled = True
        self._is_compiled = True

    def is_compiled(self) -> bool:
        """
        Checks whether a DJ AST Node is compiled
        """
        return self._is_compiled


class DJEnum(Enum):
    """
    A DJ AST enum
    """

    def __repr__(self) -> str:
        return str(self)


@dataclass(eq=False)
class Aliasable(Node):
    """
    A mixin for Nodes that are aliasable
    """

    alias: Optional["Name"] = None
    as_: Optional[bool] = None
    semantic_entity: Optional[str] = None
    semantic_type: Optional[SemanticType] = None

    def set_alias(self: TNode, alias: Optional["Name"]) -> TNode:
        self.alias = alias
        return self

    def set_as(self: TNode, as_: bool) -> TNode:
        self.as_ = as_
        return self

    def set_semantic_entity(self: TNode, semantic_entity: str) -> TNode:
        self.semantic_entity = semantic_entity
        return self

    def set_semantic_type(self: TNode, semantic_type: SemanticType) -> TNode:
        self.semantic_type = semantic_type
        return self

    @property
    def columns(self):
        """
        Returns a list of self if aliased or named else an empty list
        """
        return [self]

    @property
    def alias_or_name(self) -> "Name":
        if self.alias is not None:
            return self.alias
        elif isinstance(self, Named):
            return self.name
        else:
            raise DJParseException("Node has no alias or name.")


AliasedType = TypeVar("AliasedType", bound=Node)  # pylint: disable=C0103


@dataclass(eq=False)
class Alias(Aliasable, Generic[AliasedType]):
    """
    Wraps node types with an alias
    """

    child: AliasedType = field(default_factory=Node)

    def __str__(self) -> str:
        as_ = " AS " if self.as_ else " "
        return f"{self.child}{as_}{self.alias}"

    def is_aggregation(self) -> bool:
        return isinstance(self.child, Expression) and self.child.is_aggregation()

    @property
    def type(self) -> ColumnType:
        return self.child.type

    @property
    def columns(self):
        """
        Returns a list of self if aliased or named else an empty list
        """
        return [self]


TExpression = TypeVar("TExpression", bound="Expression")  # pylint: disable=C0103


@dataclass(eq=False)
class Expression(Node):
    """
    An expression type simply for type checking
    """

    parenthesized: Optional[bool] = field(init=False, default=None)

    @property
    def type(self) -> Union[ColumnType, List[ColumnType]]:
        """
        Return the type of the expression
        """

    @property
    def columns(self):
        """
        Returns a list of self if aliased or named else an empty list
        """
        return []

    def is_aggregation(self) -> bool:
        """
        Determines whether an Expression is an aggregation or not
        """
        for child in self.children:
            if hasattr(child, "is_aggregation") and child.is_aggregation():
                return True
        return False

    def set_alias(self: TExpression, alias: "Name") -> Alias[TExpression]:
        return Alias(child=self).set_alias(alias)

    def without_aliases(self) -> TExpression:
        exp = self
        while hasattr(exp, "alias") or isinstance(exp, Alias) or hasattr(exp, "child"):
            if hasattr(exp, "child"):
                exp = exp.child
            elif hasattr(exp, "expression"):
                exp = exp.expression
        return exp


@dataclass(eq=False)
class Name(Node):
    """
    The string name specified in sql with quote style
    """

    name: str
    quote_style: str = ""
    namespace: Optional["Name"] = None

    def __post_init__(self):
        if isinstance(self.name, Name):
            self.quote_style = self.quote_style or self.name.quote_style
            self.namespace = self.namespace or self.name.namespace
            self.name = self.name.name

    def __str__(self) -> str:
        return self.identifier(True)

    @property
    def names(self) -> List["Name"]:
        namespace = [self]
        name = self
        while name.namespace:
            namespace.append(name.namespace)
            name = name.namespace
        return namespace[::-1]

    def identifier(self, quotes: bool = True) -> str:
        """
        Yield a string of all the names making up
        the name with or without quotes
        """
        quote_style = "" if not quotes else self.quote_style
        namespace = str(self.namespace) + "." if self.namespace else ""
        return (
            f"{namespace}{quote_style}{self.name}{quote_style}"  # pylint: disable=C0301
        )


TNamed = TypeVar("TNamed", bound="Named")  # pylint: disable=C0103


@dataclass(eq=False)  # type: ignore
class Named(Node):
    """
    An Expression that has a name
    """

    name: Name

    @staticmethod
    def namespaces_intersect(
        namespace_a: List[Name],
        namespace_b: List[Name],
        quotes: bool = False,
    ):
        return all(
            na.name == nb.name if not quotes else str(na) == str(nb)
            for na, nb in zip(reversed(namespace_a), reversed(namespace_b))
        )

    @property
    def names(self) -> List[Name]:
        return self.name.names

    @property
    def namespace(self) -> List[Name]:
        return self.names[:-1]

    @property
    def columns(self):
        """
        Returns a list of self if aliased or named else an empty list
        """
        return [self]

    def identifier(self, quotes: bool = True) -> str:
        if quotes:
            return str(self.name)

        return ".".join(
            (
                *(name.name for name in self.namespace),
                self.name.name,
            ),
        )

    @property
    def alias_or_name(self) -> "Name":
        return self.name


@dataclass(eq=False)
class DefaultName(Name):
    name: str = ""

    def __bool__(self) -> bool:
        return False


@dataclass(eq=False)
class UnNamed(Named):
    name: Name = field(default_factory=DefaultName)


@dataclass(eq=False)
class Column(Aliasable, Named, Expression):
    """
    Column used in statements
    """

    _table: Optional[Union[Aliasable, "TableExpression"]] = field(
        repr=False,
        default=None,
    )
    _is_struct_ref: bool = False
    _type: Optional["ColumnType"] = field(repr=False, default=None)
    _expression: Optional[Expression] = field(repr=False, default=None)
    _is_compiled: bool = False
    role: Optional[str] = None
    dimension_ref: Optional[str] = None

    @property
    def type(self):
        if self._type:
            return self._type
        # Column was derived from some other expression we can get the type of
        if self.expression:
            self.add_type(self.expression.type)
            return self.expression.type

        parent_expr = f"in {self.parent}" if self.parent else "that has no parent"
        raise DJParseException(f"Cannot resolve type of column {self} {parent_expr}")

    def add_type(self, type_: ColumnType) -> "Column":
        """
        Add a referenced type
        """
        self._type = type_
        return self

    def copy(self):
        return Column(
            name=self.name,
            alias=self.alias,
            _table=self.table,
            _type=self.type,
            _is_compiled=self.is_compiled(),
        )

    @property
    def expression(self) -> Optional[Expression]:
        """
        Return the Expression this node points to in a subquery
        """
        return self._expression

    def namespace_table(self):
        """
        Turns the column's namespace into a table.
        """
        if self.name.namespace:
            self._table = Table(name=self.name.namespace)
            self.name.namespace = None
            self._table.parent = self
            self._table.parent_key = "_table"

    def add_expression(self, expression: "Expression") -> "Column":
        """
        Add a referenced expression where the column came from
        """
        self._expression = expression
        return self

    def set_struct_ref(self):
        """
        Marks this column as a struct dereference. This implies that we treat the name
        and namespace values on this object as struct column and struct subscript values.
        """
        self._is_struct_ref = True

    def add_table(self, table: "TableExpression"):
        self._table = table

    @property
    def table(self) -> Optional["TableExpression"]:
        """
        Return the table the column was referenced from
        """
        return self._table

    @property
    def children(self) -> Iterator[Node]:
        if self.table and self.table.parent is self:
            return chain(super().children, (self.table,))
        return super().children

    @property
    def is_api_column(self) -> bool:
        """
        Is the column added from the api?
        """
        return self._api_column

    def set_api_column(self, api_column: bool = False) -> "Column":
        """
        Set the api column flag
        """
        self._api_column = api_column
        return self

    def use_alias_as_name(self) -> "Column":
        """Use the column's alias as its name"""
        self.name = self.alias
        self.alias = None
        return self

    def is_compiled(self):
        return self._is_compiled or (self.table and self._type)

    def column_names(self) -> Tuple[Optional[str], str, Optional[str]]:
        """
        Returns the column namespace (if any), column name, and subscript name (if any)
        """
        subscript_name = None
        column_name = self.name.name
        column_namespace = None
        if len(self.namespace) == 2:  # struct
            column_namespace, column_name = self.namespace
            column_name = column_name.name
            subscript_name = self.name.name
        elif len(self.namespace) == 1:  # non-struct
            column_namespace = self.namespace[0].name
        return column_namespace, column_name, subscript_name

    async def find_table_sources(
        self,
        ctx: CompileContext,
    ) -> List["TableExpression"]:
        # flake8: noqa
        """
        Find all tables that this column could have originated from.
        """
        query = cast(
            Query,
            self.get_nearest_parent_of_type(Query),
        )
        direct_tables = list(
            filter(
                lambda tbl: tbl.in_from_or_lateral()
                and tbl.get_nearest_parent_of_type(Query) is query,
                query.find_all(TableExpression),
            ),
        )
        if hasattr(self, "child"):
            self.add_type(self.child.type)
        for table in direct_tables:
            if not table.is_compiled():
                await table.compile(ctx)

        namespace = (
            self.name.namespace.identifier(False) if self.name.namespace else ""
        )  # a.x -> a

        # Determine if the column is referencing a struct
        column_namespace, column_name, subscript_name = self.column_names()
        is_struct = column_namespace and column_name and subscript_name

        found = []

        # Go through TableExpressions directly on the AST first and collect all
        # possible origins for this column. There may be more than one if the column
        # is not namespaced.
        for table in direct_tables:
            if (
                # This column may be namespaced, in which case we'll search for an origin table
                # that has the namespace as an alias or name. If this column has no namespace,
                # it should be sourced from the immediate table
                not namespace
                or namespace == table.alias_or_name.identifier(False)
            ):
                result = await table.add_ref_column(self, ctx)
                if result:
                    found.append(table)

        # This column may be a struct, meaning it'll have an optional namespace,
        # a column name, and a subscript
        if not found:
            for table in direct_tables:
                namespace = namespace.split(SEPARATOR)[0]
                if table.column_mapping.get(namespace) or (
                    table.column_mapping.get(column_name)
                    and is_struct
                    and (
                        not namespace
                        or table.alias_or_name.namespace
                        and table.alias_or_name.namespace.identifier() == namespace
                    )
                ):
                    if await table.add_ref_column(self, ctx):
                        found.append(table)

        if found:
            return found

        if not query.in_from_or_lateral():
            correlation_tables = list(
                filter(
                    lambda tbl: tbl.in_from_or_lateral()
                    and query.is_ancestor_of(tbl.get_nearest_parent_of_type(Query)),
                    query.find_all(TableExpression),
                ),
            )
            for table in correlation_tables:
                if not namespace or table.alias_or_name.identifier(False) == namespace:
                    if await table.add_ref_column(self, ctx):
                        found.append(table)

        if found:
            return found

        # Check for ctes
        alpha_query = self.get_furthest_parent()
        direct_table_names = {
            direct_table.alias_or_name.identifier() for direct_table in direct_tables
        }
        if isinstance(alpha_query, Query) and alpha_query.ctes:
            for cte in alpha_query.ctes:
                cte_name = cte.alias_or_name.identifier(False)
                if cte_name == namespace or (
                    not namespace and cte_name in direct_table_names
                ):
                    if await cte.add_ref_column(self, ctx):
                        found.append(cte)

        # If nothing was found in the initial AST, traverse through dimensions graph
        # to find another table in DJ that could be its origin
        to_process = collections.deque([(table, []) for table in direct_tables])
        processed = set()
        while to_process:
            current_table, path = to_process.pop()
            if current_table in processed:
                continue
            processed.add(current_table)
            if (
                not namespace
                or current_table.alias_or_name.identifier(False) == namespace
            ):
                if current_table.add_ref_column(self, ctx):
                    found.append(current_table)
                    return found

            # If the table has a DJ node, check to see if the DJ node has dimensions,
            # which would link us to new nodes to search for this column in
            if isinstance(current_table, Table) and current_table.dj_node:
                if not isinstance(current_table.dj_node, NodeRevision):
                    current_table.set_dj_node(current_table.dj_node.current)
                for dj_col in current_table.dj_node.columns:
                    if dj_col.dimension:
                        col_dimension = await DJNodeRef.get_by_name(
                            ctx.session,
                            dj_col.dimension.name,
                            options=[
                                joinedload(DJNodeRef.current).options(
                                    selectinload(DJNode.columns),
                                ),
                            ],
                        )
                        if col_dimension:
                            new_table = Table(
                                name=to_namespaced_name(col_dimension.name),
                                _dj_node=col_dimension.current,
                                path=path,
                            )
                            new_table._columns = [
                                Column(
                                    name=Name(col.name),
                                    _type=col.type,
                                    _table=new_table,
                                )
                                for col in col_dimension.current.columns
                            ]
                            to_process.append((new_table, path))
                await ctx.session.refresh(current_table.dj_node, ["dimension_links"])
                for link in current_table.dj_node.dimension_links:
                    all_roles = []
                    if self.role:
                        all_roles = self.role.split(" -> ")
                    if (not link.role and not all_roles) or (link.role in all_roles):
                        await ctx.session.refresh(link, ["dimension"])
                        if link.dimension:
                            await ctx.session.refresh(link.dimension, ["current"])
                            new_table = Table(
                                name=to_namespaced_name(link.dimension.name),
                                _dj_node=link.dimension.current,
                                dimension_link=link,
                                path=path + [link],
                            )
                            await ctx.session.refresh(
                                link.dimension.current,
                                ["columns"],
                            )
                            new_table._columns = [
                                Column(
                                    name=Name(col.name),
                                    _type=col.type,
                                    _table=new_table,
                                )
                                for col in link.dimension.current.columns
                            ]
                            to_process.append((new_table, path + [link]))
        return found

    async def compile(self, ctx: CompileContext):
        """
        Compile a column.
        Determines the table from which a column is from.
        """

        if self.is_compiled():
            return

        # check if the column was already given a table
        if self.table and isinstance(self.table.parent, Column):
            await self.table.add_ref_column(self, ctx)
        else:
            found_sources = await self.find_table_sources(ctx)
            if len(found_sources) < 1:
                ctx.exception.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_COLUMN,
                        message=f"Column `{self}` does not exist on any valid table.",
                    ),
                )
                return

            if len(found_sources) > 1:
                ctx.exception.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_COLUMN,
                        message=f"Column `{self.name.name}` found in multiple tables. Consider using fully qualified name.",
                    ),
                )
                return

            source_table = cast(TableExpression, found_sources[0])
            added = await source_table.add_ref_column(self, ctx)
            if not added:
                ctx.exception.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_COLUMN,
                        message=f"Column `{self}` does not exist on any valid table.",
                    ),
                )
                return
        self._is_compiled = True

    @property
    def struct_column_name(self) -> str:
        """If this is a struct reference, the struct type's column name"""
        column_namespace, column_name, subscript_name = self.column_names()
        if len(self.namespace) == 1:  # non-struct
            return column_namespace
        return column_name

    @property
    def struct_subscript(self) -> str:
        """If this is a struct reference, the struct type's field name"""
        return self.name.name

    def __str__(self) -> str:
        as_ = " AS " if self.as_ else " "
        alias = "" if not self.alias else f"{as_}{self.alias}"
        if self.table is not None and not isinstance(self.table, FunctionTable):
            name = (
                self.struct_column_name + "." + self.struct_subscript
                if self._is_struct_ref
                else self.name.name
            )
            ret = f"{self.name.quote_style}{name}{self.name.quote_style}"
            if table_name := self.table.alias_or_name:
                ret = (
                    (
                        table_name
                        if isinstance(table_name, str)
                        else table_name.identifier()
                    )
                    + "."
                    + ret
                )
        else:
            ret = str(self.name)
        if self.parenthesized:
            ret = f"({ret})"
        return ret + alias

    @property
    def is_struct_ref(self):
        return self._is_struct_ref


@dataclass(eq=False)
class Wildcard(Named, Expression):
    """
    Wildcard or '*' expression
    """

    name: Name = field(init=False, repr=False, default=Name("*"))
    _table: Optional["Table"] = field(repr=False, default=None)

    @property
    def table(self) -> Optional["Table"]:
        """
        Return the table the column was referenced from if there's one
        """
        return self._table

    def add_table(self, table: "Table") -> "Wildcard":
        """
        Add a referenced table
        """
        if self._table is None:
            self._table = table
        return self

    def __str__(self) -> str:
        return "*"

    @property
    def type(self) -> ColumnType:
        return WildcardType()


@dataclass(eq=False)
class TableExpression(Aliasable, Expression):
    """
    A type for table expressions
    """

    column_list: List[Column] = field(default_factory=list)
    _columns: List[Expression] = field(
        default_factory=list,
    )  # all those expressions that can be had from the table; usually derived from dj node metadata for Table
    # ref (referenced) columns are columns used elsewhere from this table
    _ref_columns: List[Column] = field(init=False, repr=False, default_factory=list)

    @property
    def columns(self) -> List[Expression]:
        """
        Return the columns named in this table
        """
        col_list_names = {col.name.name for col in self.column_list}
        return [
            col
            for col in self._columns
            if isinstance(col, (Aliasable, Named))
            and (col.alias_or_name.name in col_list_names if col_list_names else True)
        ]

    @property
    def column_mapping(self) -> Dict[str, Column]:
        """
        Return the columns named in this table
        """
        return {
            col.alias_or_name.name: col
            for col in self._columns
            if isinstance(col, (Aliasable, Named))
        }

    @property
    def ref_columns(self) -> Set[Column]:
        """
        Return the columns referenced from this table
        """
        return self._ref_columns

    async def add_ref_column(
        self,
        column: Column,
        ctx: Optional[CompileContext] = None,
    ) -> bool:
        """
        Add column referenced from this table. Returns True if the table has the column
        and False otherwise.

        This function handles the following cases:

        Regular columns. For example:
        (1) non-aliased columns
          `SELECT country_id AS country1 FROM countries` should match the `country_id`
          column in the table `countries`
        (2) aliased columns
          `SELECT C.country_id AS country1 FROM countries C` should match the `country_id`
          column in the table `countries` with the column namespace/table alias `C`

        Struct columns. For example:
        (1) non-aliased struct columns
          `countries` has column `identifiers` with type:
                STRUCT<country_name STR, country_code STR>
          `SELECT identifiers.country_name AS name FROM countries` should match the
          `identifier` -> `country_name` column in the table `countries`
        (2) aliased struct columns
          `countries` has column `identifiers` with type:
                STRUCT<country_name STR, country_code STR>
          `SELECT C.identifiers.country_name AS name FROM countries C` should match the
          `identifier` -> `country_name` column in the table `countries` with the column namespace/
          table alias `C`
        """
        if not self._columns:
            if ctx is None:
                raise DJErrorException(
                    "Uncompiled Table expression requested to "
                    "add Column ref without a compilation context.",
                )
            await self.compile(ctx)
        return self.add_column_reference(column)

    def add_column_reference(
        self,
        column: Column,
    ) -> bool:
        """
        Add column referenced from this table. Returns True if the table has the column
        and False otherwise.
        """
        if not isinstance(column, Alias):
            ref_col_name = column.name.name
            if matching_column := self.column_mapping.get(ref_col_name):
                self._ref_columns.append(column)
                column.add_table(self)
                column.add_expression(matching_column)
                column.add_type(matching_column.type)

        # For table-valued functions, add the list of columns that gets
        # returned as reference columns and compile them
        if isinstance(self, FunctionTable):
            if (
                not self.alias
                and not column.name.namespace
                or (
                    self.alias
                    and column.name.namespace
                    and self.alias == column.name.namespace
                )
            ):
                for col in self.column_list:
                    if column.name.name == col.alias_or_name.name:
                        self._ref_columns.append(column)
                        column.add_table(self)
                        column.add_expression(col)
                        column.add_type(col.type)
                        return True

        for col in self.columns:
            if isinstance(col, (Aliasable, Named)):
                if (
                    not column.alias
                    and not hasattr(column, "child")
                    and column.name.name == col.alias_or_name.name
                ):
                    self._ref_columns.append(column)
                    column.add_table(self)
                    column.add_expression(col)
                    column.add_type(col.type)
                    return True

                # For struct types we can additionally check if there's a column that matches
                # the search column's namespace and if there's a nested field that matches the
                # search column's name
                if (
                    hasattr(col, "_type")
                    and col._type
                    and isinstance(col.type, StructType)
                ) or (not hasattr(col, "_type") and isinstance(col.type, StructType)):
                    # struct column name
                    column_namespace = ".".join(
                        [name.name for name in column.namespace],
                    )
                    subscript_name = column.name.name
                    column_name = column.name.name
                    if len(column.namespace) == 2:
                        column_namespace, column_name, _ = column.column_names()

                    if col.alias_or_name.identifier(False) in (
                        column_namespace,
                        column_name,
                    ):
                        for type_field in col.type.fields:
                            if type_field.name.name == subscript_name:
                                self._ref_columns.append(column)
                                column.set_struct_ref()
                                column.add_table(self)
                                column.add_expression(col)
                                column.add_type(type_field.type)
                                return True
        return False

    def is_compiled(self) -> bool:
        return bool(self._columns) or self._is_compiled

    def in_from_or_lateral(self) -> bool:
        """
        Determines if the table expression is referenced in a From clause
        """

        if from_ := self.get_nearest_parent_of_type(From):
            return from_.get_nearest_parent_of_type(
                Select,
            ) is self.get_nearest_parent_of_type(Select)
        if lateral := self.get_nearest_parent_of_type(LateralView):
            return lateral.get_nearest_parent_of_type(
                Select,
            ) is self.get_nearest_parent_of_type(Select)
        return False


@dataclass(eq=False)
class Table(TableExpression, Named):
    """
    A type for tables
    """

    _dj_node: Optional[DJNode] = field(repr=False, default=None)
    dimension_link: Optional[DimensionLink] = field(repr=False, default=None)
    path: Optional[List["Table"]] = field(repr=False, default=None)

    @property
    def dj_node(self) -> Optional[DJNode]:
        """
        Return the dj_node referenced by this table
        """
        return self._dj_node

    def set_dj_node(self, dj_node: DJNode) -> "Table":
        """
        Set dj_node referenced by this table
        """
        self._dj_node = dj_node
        return self

    def __str__(self) -> str:
        table_str = str(self.name)
        if self.alias:
            as_ = " AS " if self.as_ else " "
            table_str += f"{as_}{self.alias}"
        return table_str

    def is_compiled(self) -> bool:
        return super().is_compiled() and (self.dj_node is not None)

    def set_alias(self: TNode, alias: "Name") -> TNode:
        self.alias = alias
        for col in self._columns:
            if col.table:
                col.table.alias = self.alias
        return self

    async def compile(self, ctx: CompileContext):
        # things we can validate here:
        # - if the node is a dimension in a groupby, is it joinable?
        self._is_compiled = True
        try:
            if not self.dj_node:
                dj_node = await get_dj_node(
                    ctx.session,
                    self.identifier(quotes=False),
                    {DJNodeType.SOURCE, DJNodeType.TRANSFORM, DJNodeType.DIMENSION},
                )
                self.set_dj_node(dj_node)
            self._columns = [
                Column(Name(col.name), _type=col.type, _table=self)
                for col in self.dj_node.columns
            ]
        except DJErrorException as exc:
            ctx.exception.errors.append(exc.dj_error)


class Operation(Expression):
    """
    A type to overarch types that operate on other expressions
    """


# pylint: disable=C0103
class UnaryOpKind(DJEnum):
    """
    The accepted unary operations
    """

    Exists = "EXISTS"
    Not = "NOT"

    def __str__(self):
        return self.value


@dataclass(eq=False)
class UnaryOp(Operation):
    """
    An operation that operates on a single expression
    """

    op: UnaryOpKind
    expr: Expression

    def __str__(self) -> str:
        ret = f"{self.op} {self.expr}"
        if self.parenthesized:
            return f"({ret})"
        return ret

    @property
    def type(self) -> ColumnType:
        type_ = self.expr.type

        def raise_unop_exception():
            raise DJParseException(
                "Incompatible type in unary operation "
                f"{self}. Got {type} in {self}.",
            )

        if self.op == UnaryOpKind.Not:
            if isinstance(type_, BooleanType):
                return type_
            raise_unop_exception()
        if self.op == UnaryOpKind.Exists:
            if isinstance(type_, BooleanType):
                return type_
            raise_unop_exception()

        raise DJParseException(f"Unary operation {self.op} not supported!")


# pylint: disable=C0103
class BinaryOpKind(DJEnum):
    """
    The DJ AST accepted binary operations
    """

    And = "AND"
    LogicalAnd = "&&"
    Or = "OR"
    LogicalOr = "||"
    Is = "IS"
    Eq = "="
    NotEq = "<>"
    NotEquals = "!="
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
    NullSafeEq = "<=>"


@dataclass(eq=False)
class BinaryOp(Operation):
    """
    Represents an operation that operates on two expressions
    """

    op: BinaryOpKind
    left: Expression
    right: Expression
    use_alias_as_name: Optional[bool] = False

    @classmethod
    def And(  # pylint: disable=invalid-name,keyword-arg-before-vararg
        cls,
        left: Expression,
        right: Optional[Expression] = None,
        *rest: Expression,
    ) -> Union["BinaryOp", Expression]:
        """
        Create a BinaryOp of kind BinaryOpKind.Eq rolling up all expressions
        """
        if right is None:  # pragma: no cover
            return left
        return reduce(
            lambda left, right: BinaryOp(
                BinaryOpKind.And,
                left,
                right,
            ),
            (left, right, *rest),
        )

    @classmethod
    def Eq(  # pylint: disable=invalid-name
        cls,
        left: Expression,
        right: Optional[Expression],
        use_alias_as_name: Optional[bool] = False,
    ) -> Union["BinaryOp", Expression]:
        """
        Create a BinaryOp of kind BinaryOpKind.Eq
        """
        if right is None:  # pragma: no cover
            return left
        return BinaryOp(
            BinaryOpKind.Eq,
            left,
            right,
            use_alias_as_name=use_alias_as_name,
        )

    def __str__(self) -> str:
        left, right = self.left, self.right
        if self.use_alias_as_name:
            if isinstance(self.right, Column) and self.right.alias:
                right = self.right.copy().use_alias_as_name()
            if isinstance(self.left, Column) and self.left.alias:
                left = self.left.copy().use_alias_as_name()
        ret = f"{left} {self.op.value} {right}"

        if self.parenthesized:
            return f"({ret})"
        return ret

    @property
    def type(self) -> ColumnType:
        kind = self.op
        left_type = self.left.type
        right_type = self.right.type

        def raise_binop_exception():
            raise DJParseException(
                "Incompatible types in binary operation "
                f"{self}. Got left {left_type}, right {right_type}.",
            )

        numeric_types = {
            type_: idx
            for idx, type_ in enumerate(
                [
                    str(DoubleType()),
                    str(FloatType()),
                    str(BigIntType()),
                    str(IntegerType()),
                ],
            )
        }

        def resolve_numeric_types_binary_operations(
            left: ColumnType,
            right: ColumnType,
        ):
            if not left.is_compatible(right):
                raise_binop_exception()
            if str(left) in numeric_types and str(right) in numeric_types:
                if str(left) == str(right):
                    return left
                if numeric_types[str(left)] > numeric_types[str(right)]:
                    return right
                return left
            return left

        BINOP_TYPE_COMBO_LOOKUP: Dict[  # pylint: disable=C0103
            BinaryOpKind,
            Callable[[ColumnType, ColumnType], ColumnType],
        ] = {
            BinaryOpKind.And: lambda left, right: BooleanType(),
            BinaryOpKind.Or: lambda left, right: BooleanType(),
            BinaryOpKind.Is: lambda left, right: BooleanType(),
            BinaryOpKind.Eq: lambda left, right: BooleanType(),
            BinaryOpKind.NotEq: lambda left, right: BooleanType(),
            BinaryOpKind.NotEquals: lambda left, right: BooleanType(),
            BinaryOpKind.Gt: lambda left, right: BooleanType(),
            BinaryOpKind.Lt: lambda left, right: BooleanType(),
            BinaryOpKind.GtEq: lambda left, right: BooleanType(),
            BinaryOpKind.LtEq: lambda left, right: BooleanType(),
            BinaryOpKind.BitwiseOr: lambda left, right: IntegerType()
            if str(left) == str(IntegerType()) and str(right) == str(IntegerType())
            else raise_binop_exception(),
            BinaryOpKind.BitwiseAnd: lambda left, right: IntegerType()
            if str(left) == str(IntegerType()) and str(right) == str(IntegerType())
            else raise_binop_exception(),
            BinaryOpKind.BitwiseXor: lambda left, right: IntegerType()
            if str(left) == str(IntegerType()) and str(right) == str(IntegerType())
            else raise_binop_exception(),
            BinaryOpKind.Multiply: resolve_numeric_types_binary_operations,
            BinaryOpKind.Divide: resolve_numeric_types_binary_operations,
            BinaryOpKind.Plus: resolve_numeric_types_binary_operations,
            BinaryOpKind.Minus: resolve_numeric_types_binary_operations,
            BinaryOpKind.Modulo: lambda left, right: IntegerType()
            if str(left) == str(IntegerType()) and str(right) == str(IntegerType())
            else raise_binop_exception(),
        }
        return BINOP_TYPE_COMBO_LOOKUP[kind](left_type, right_type)

    async def compile(self, ctx: CompileContext):
        """
        Compile a DJ Node. By default, we call compile on all immediate children of this node.
        """
        if self._is_compiled:
            return
        for child in self.children:
            if not child.is_compiled():
                await child.compile(ctx)
                child._is_compiled = True
        self._is_compiled = True


@dataclass(eq=False)
class FrameBound(Expression):
    """
    Represents frame bound in a window function
    """

    start: str
    stop: str

    def __str__(self) -> str:
        return f"{self.start} {self.stop}"


@dataclass(eq=False)
class Frame(Expression):
    """
    Represents frame in window function
    """

    frame_type: str
    start: FrameBound
    end: Optional[FrameBound] = None

    def __str__(self) -> str:
        end = f" AND {self.end}" if self.end else ""
        between = " BETWEEN" if self.end else ""
        return f"{self.frame_type}{between} {self.start}{end}"


@dataclass(eq=False)
class Over(Expression):
    """
    Represents a function used in a statement
    """

    partition_by: List[Expression] = field(default_factory=list)
    order_by: List["SortItem"] = field(default_factory=list)
    window_frame: Optional[Frame] = None

    def __str__(self) -> str:
        partition_by = (  # pragma: no cover
            " PARTITION BY " + ", ".join(str(exp) for exp in self.partition_by)
            if self.partition_by
            else ""
        )
        order_by = (
            " ORDER BY " + ", ".join(str(exp) for exp in self.order_by)
            if self.order_by
            else ""
        )
        consolidated_by = "\n".join(
            po_by for po_by in (partition_by, order_by) if po_by
        )
        window_frame = f" {self.window_frame}" if self.window_frame else ""
        return f"OVER ({consolidated_by}{window_frame})"


@dataclass(eq=False)
class Function(Named, Operation):
    """
    Represents a function used in a statement
    """

    args: List[Expression] = field(default_factory=list)
    quantifier: str = ""
    over: Optional[Over] = None
    args_compiled: bool = False

    def __new__(
        cls,
        name: Name,
        args: List[Expression],
        quantifier: str = "",
        over: Optional[Over] = None,
    ):
        # Check if function is a table-valued function
        if (
            not quantifier
            and over is None
            and name.name.upper() in table_function_registry
        ):
            return FunctionTable(name, args=args)

        # If not, create a new Function object
        return super().__new__(cls)

    def __getnewargs__(self):
        return self.name, self.args

    def __deepcopy__(self, memodict):
        return self

    def __str__(self) -> str:
        if self.name.name.upper() in function_registry and self.is_runtime():
            return self.function().substitute()

        over = f" {self.over} " if self.over else ""
        quantifier = f" {self.quantifier} " if self.quantifier else ""
        ret = (
            f"{self.name}({quantifier}{', '.join(str(arg) for arg in self.args)}){over}"
        )
        if self.parenthesized:
            ret = f"({ret})"
        return ret

    def function(self):
        return function_registry[self.name.name.upper()]

    def is_aggregation(self) -> bool:
        if self.function().is_aggregation:
            return True
        return super().is_aggregation()

    def is_runtime(self) -> bool:
        return self.function().is_runtime

    @property
    def type(self) -> ColumnType:
        return self.function().infer_type(*self.args)

    async def compile(self, ctx: CompileContext):
        """
        Compile a function
        """
        self._is_compiled = True
        for arg in self.args:
            if not arg.is_compiled():
                await arg.compile(ctx)
                arg._is_compiled = True

        # FIXME: We currently catch this exception because we are unable  # pylint: disable=fixme
        # to infer types for nested lambda functions. For the time being, an easy workaround is to
        # add a CAST(...) wrapper around the nested lambda function so that the type hard-coded by
        # the argument to CAST
        try:
            self.function().compile_lambda(*self.args)
        except DJParseException as parse_exc:
            if "Cannot resolve type of column" in parse_exc.message:
                logger.warning(parse_exc)
            else:
                raise parse_exc

        for child in self.children:
            if not child.is_compiled():
                await child.compile(ctx)
                child._is_compiled = True


class Value(Expression):
    """
    Base class for all values number, string, boolean
    """

    def is_aggregation(self) -> bool:
        return False


@dataclass(eq=False)
class Null(Value):
    """
    Null value
    """

    def __str__(self) -> str:
        return "NULL"

    @property
    def type(self) -> ColumnType:
        return NullType()


@dataclass(eq=False)
class Number(Value):
    """
    Number value
    """

    value: Union[float, int, decimal.Decimal]
    _type: Optional[IntegerBase] = None

    def __post_init__(self):
        super().__post_init__()

        if (
            not isinstance(self.value, float)
            and not isinstance(self.value, int)
            and not isinstance(self.value, decimal.Decimal)
        ):
            cast_exceptions = []
            numeric_types = [int, float, decimal.Decimal]
            for cast_type in numeric_types:
                try:
                    self.value = cast_type(self.value)
                    break
                except (ValueError, OverflowError) as exception:
                    cast_exceptions.append(exception)
            if len(cast_exceptions) >= len(numeric_types):
                raise DJException(message="Not a valid number!")

    def __str__(self) -> str:
        return str(self.value)

    @property
    def type(self) -> ColumnType:
        """
        Determine the type of the numeric expression.
        """
        # We won't assume that anyone wants SHORT by default
        if isinstance(self.value, int):
            check_types = (self._type,) if self._type else (IntegerType(), BigIntType())
            for integer_type in check_types:
                if integer_type.check_bounds(self.value):
                    return integer_type

            raise DJParseException(
                f"No Integer type of {check_types} can hold the value {self.value}.",
            )
        #
        # # Arbitrary-precision floating point
        # if isinstance(self.value, decimal.Decimal):
        #     return DecimalType.parse(self.value)

        # Double-precision floating point
        if not (1.18e-38 <= abs(self.value) <= 3.4e38):
            return DoubleType()

        # Single-precision floating point
        return FloatType()


@dataclass(eq=False)
class String(Value):
    """
    String value
    """

    value: str

    def __str__(self) -> str:
        return self.value

    @property
    def type(self) -> ColumnType:
        return StringType()


@dataclass(eq=False)
class Boolean(Value):
    """
    Boolean True/False value
    """

    value: bool

    def __str__(self) -> str:
        return str(self.value)

    @property
    def type(self) -> ColumnType:
        return BooleanType()


@dataclass(eq=False)
class IntervalUnit(Value):
    """
    Interval unit value
    """

    unit: str
    value: Optional[Number] = None

    def __str__(self) -> str:
        return f"{self.value or ''} {self.unit}"


@dataclass(eq=False)
class Interval(Value):
    """
    Interval value
    """

    from_: List[IntervalUnit]
    to: Optional[IntervalUnit] = None

    def __str__(self) -> str:
        to = f"TO {self.to}" if self.to else ""
        return f"INTERVAL {' '.join(str(interval) for interval in self.from_)} {to}"

    @property
    def type(self) -> ColumnType:
        """
        Determine the type of the interval expression.
        """
        units = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]
        years_months_units = {"YEAR", "MONTH"}
        days_seconds_units = {"DAY", "HOUR", "MINUTE", "SECOND"}
        from_ = [DateTimeBase.Unit(f.unit) for f in self.from_]
        if all(unit.unit in years_months_units for unit in self.from_):
            if (
                self.to is None
                or self.to.unit is None
                or self.to.unit in years_months_units
            ):
                # If all the units in the from_ list are YEAR or MONTH, the interval
                # is a YearMonthInterval
                return YearMonthIntervalType(
                    sorted(from_, key=lambda u: units.index(u))[0],
                    self.to,
                )
        elif all(unit.unit in days_seconds_units for unit in self.from_):
            if (
                self.to is None
                or self.to.unit is None
                or self.to.unit in days_seconds_units
            ):
                # If the to_ attribute is None or its unit is DAY, HOUR, MINUTE, or
                # SECOND, the interval is a DayTimeInterval
                return DayTimeIntervalType(
                    sorted(from_, key=lambda u: units.index(u))[0],
                    self.to,
                )
        raise DJParseException(f"Invalid interval type specified in {self}.")


@dataclass(eq=False)
class Struct(Value):
    """
    Struct value
    """

    values: List[Aliasable]

    def __str__(self):
        inner = ", ".join(str(value) for value in self.values)
        return f"STRUCT({inner})"


@dataclass(eq=False)
class Predicate(Operation):
    """
    Represents a predicate
    """

    negated: bool = False

    @property
    def type(self) -> ColumnType:
        return BooleanType()


@dataclass(eq=False)
class Between(Predicate):
    """
    A between statement
    """

    expr: Expression = field(default_factory=Expression)
    low: Expression = field(default_factory=Expression)
    high: Expression = field(default_factory=Expression)

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        between = f"{not_}{self.expr} BETWEEN {self.low} AND {self.high}"
        if self.parenthesized:
            between = f"({between})"
        return between

    @property
    def type(self) -> ColumnType:
        expr_type = self.expr.type
        low_type = self.low.type
        high_type = self.high.type
        if expr_type == low_type == high_type:
            return BooleanType()
        raise DJParseException(
            f"BETWEEN expects all elements to have the same type got "
            f"{expr_type} BETWEEN {low_type} AND {high_type} in {self}.",
        )


@dataclass(eq=False)
class In(Predicate):
    """
    An in expression
    """

    expr: Expression = field(default_factory=Expression)
    source: Union[List[Expression], "Select"] = field(default_factory=Expression)

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        source = (
            str(self.source)
            if isinstance(self.source, Select)
            else "(" + ", ".join(str(exp) for exp in self.source) + ")"
        )
        return f"{self.expr} {not_}IN {source}"


@dataclass(eq=False)
class Rlike(Predicate):
    """
    A regular expression match statement
    """

    expr: Expression = field(default_factory=Expression)
    pattern: Expression = field(default_factory=Expression)

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        return f"{not_}{self.expr} RLIKE {self.pattern}"


@dataclass(eq=False)
class Like(Predicate):
    """
    A string pattern matching statement
    """

    expr: Expression = field(default_factory=Expression)
    quantifier: str = ""
    patterns: List[Expression] = field(default_factory=list)
    escape_char: Optional[str] = None
    case_sensitive: Optional[bool] = True

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        if self.quantifier:  # quantifier means a pattern with multiple elements
            pattern = f'({", ".join(str(p) for p in self.patterns)})'
        else:
            pattern = self.patterns
        escape_char = f" ESCAPE '{self.escape_char}'" if self.escape_char else ""
        quantifier = f"{self.quantifier} " if self.quantifier else ""
        like_type = "LIKE" if self.case_sensitive else "ILIKE"
        return f"{not_}{self.expr} {like_type} {quantifier}{pattern}{escape_char}"

    @property
    def type(self) -> ColumnType:
        expr_type = self.expr.type
        if expr_type == StringType():
            return BooleanType()
        raise DJParseException(
            f"Incompatible type for {self}: {expr_type}. Expected STR",
        )


@dataclass(eq=False)
class IsNull(Predicate):
    """
    A null check statement
    """

    expr: Expression = field(default_factory=Expression)

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        return f"{self.expr} IS {not_}NULL"

    @property
    def type(self) -> ColumnType:
        return BooleanType()


@dataclass(eq=False)
class IsBoolean(Predicate):
    """
    A boolean check statement
    """

    expr: Expression = field(default_factory=Expression)
    value: str = "UNKNOWN"

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        return f"{self.expr} IS {not_}{self.value}"

    @property
    def type(self) -> ColumnType:
        return BooleanType()


@dataclass(eq=False)
class IsDistinctFrom(Predicate):
    """
    A distinct from check statement
    """

    expr: Expression = field(default_factory=Expression)
    right: Expression = field(default_factory=Expression)

    def __str__(self) -> str:
        not_ = "NOT " if self.negated else ""
        return f"{self.expr} IS {not_}DISTINCT FROM {self.right}"

    @property
    def type(self) -> ColumnType:
        return BooleanType()


@dataclass(eq=False)
class Case(Expression):
    """
    A case statement of branches
    """

    expr: Optional[Expression] = None
    conditions: List[Expression] = field(default_factory=list)
    else_result: Optional[Expression] = None
    operand: Optional[Expression] = None
    results: List[Expression] = field(default_factory=list)

    def __str__(self) -> str:
        branches = "\n\tWHEN ".join(
            f"{(cond)} THEN {(result)}"
            for cond, result in zip(self.conditions, self.results)
        )
        else_ = f"ELSE {(self.else_result)}" if self.else_result else ""
        expr = "" if self.expr is None else f" {self.expr} "
        ret = f"""CASE {expr}
        WHEN {branches}
        {else_}
    END"""
        if self.parenthesized:
            return f"({ret})"
        return ret

    def is_aggregation(self) -> bool:
        return all(result.is_aggregation() for result in self.results) and (
            self.else_result.is_aggregation() if self.else_result else True
        )

    @property
    def type(self) -> ColumnType:
        result_types = [
            res.type
            for res in self.results + ([self.else_result] if self.else_result else [])
            if res.type
        ]
        if not all(result_types[0].is_compatible(res) for res in result_types):
            raise DJParseException(
                f"Not all the same type in CASE! Found: {', '.join([str(type_) for type_ in result_types])}",
            )
        return result_types[0]


@dataclass(eq=False)
class Subscript(Expression):
    """
    Represents a subscript expression
    """

    expr: Expression
    index: Expression

    def __str__(self) -> str:
        return f"{self.expr}[{self.index}]"

    @property
    def type(self) -> ColumnType:
        if isinstance(self.expr.type, MapType):
            type_ = cast(MapType, self.expr.type)
            return type_.value.type
        if isinstance(self.expr.type, StructType):
            nested_field = self.expr.type.fields_mapping.get(
                self.index.value.replace("'", ""),
            )
            return nested_field.type
        return cast(ListType, self.expr.type).element.type


@dataclass(eq=False)
class Lambda(Expression):
    """
    Represents a lambda expression
    """

    identifiers: List[Named]
    expr: Expression

    def __str__(self) -> str:
        if len(self.identifiers) == 1:
            id_str = self.identifiers[0]
        else:
            id_str = "(" + ", ".join(str(iden) for iden in self.identifiers) + ")"
        return f"{id_str} -> {self.expr}"

    @property
    def type(self) -> Union[ColumnType, List[ColumnType]]:
        """
        The return type of the lambda function
        """
        return self.expr.type


@dataclass(eq=False)
class JoinCriteria(Node):
    """
    Represents the criteria for a join relation in a FROM clause
    """

    on: Optional[Expression] = None
    using: Optional[List[Named]] = None

    def __str__(self) -> str:
        if self.on:
            return f"ON {self.on}"
        else:
            id_list = ", ".join(str(iden) for iden in self.using)
            return f"USING ({id_list})"


@dataclass(eq=False)
class Join(Node):
    """
    Represents a join relation in a FROM clause
    """

    join_type: str
    right: Expression
    criteria: Optional[JoinCriteria] = None
    lateral: bool = False
    natural: bool = False

    def __str__(self) -> str:
        parts = []
        if self.natural:
            parts.append("NATURAL")
        if self.join_type:
            parts.append(f"{str(self.join_type).upper().strip()}")
        parts.append("JOIN")
        if self.lateral:
            parts.append("LATERAL")
        parts.append(str(self.right))
        if self.criteria:
            parts.append(f"{self.criteria}")
        return " ".join(parts)


@dataclass(eq=False)
class InlineTable(TableExpression, Named):
    """
    An inline table
    """

    values: List[Expression] = field(default_factory=list)
    explicit_columns: bool = False

    def __str__(self) -> str:
        values = "VALUES " + ",\n\t".join(
            [f'({", ".join([str(col) for col in row])})' for row in self.values],
        )
        inline_alias = self.alias_or_name.name if self.alias_or_name else ""
        alias = inline_alias + (
            f"({', '.join([col.alias_or_name.name for col in self.columns])})"
            if self.explicit_columns
            else ""
        )
        return f"{values} AS {alias}" if alias else values


@dataclass(eq=False)
class FunctionTableExpression(TableExpression, Named, Operation):
    """
    An uninitializable Type for FunctionTable for use as a
    default where a FunctionTable is required but succeeds optional fields
    """

    args: List[Expression] = field(default_factory=list)


class FunctionTable(FunctionTableExpression):
    """
    Represents a table-valued function used in a statement
    """

    def __str__(self) -> str:
        alias = f" {self.alias}" if self.alias else ""
        as_ = " AS " if self.as_ else ""
        cols = (
            f" {', '.join(col.name.name for col in self.column_list)}"
            if self.column_list
            else ""
        )

        column_parens = False
        if self.name.name.upper() == "UNNEST" or (
            self.name.name.upper() == "EXPLODE"
            and not isinstance(self.parent, LateralView)
        ):
            column_parens = True

        column_list_str = f"({cols})" if column_parens else f"{cols}"
        args_str = f"({', '.join(str(col) for col in self.args)})" if self.args else ""
        return f"{self.name}{args_str}{alias}{as_}{column_list_str}"

    def set_alias(self: TNode, alias: Name) -> TNode:
        self.alias = alias
        return self

    async def _type(self, ctx: Optional[CompileContext] = None) -> List[NestedField]:
        name = self.name.name.upper()
        dj_func = table_function_registry[name]
        arg_types = []
        for arg in self.args:
            if ctx:
                await arg.compile(ctx)
            arg_types.append(arg.type)
        return dj_func.infer_type(*arg_types)

    async def compile(self, ctx):
        if self.is_compiled():
            return
        self._is_compiled = True
        types = await self._type(ctx)
        for type, col in zip_longest(types, self.column_list):
            if self.column_list:
                if (type is None) or (col is None):
                    ctx.exception.errors.append(
                        DJError(
                            code=ErrorCode.INVALID_SQL_QUERY,
                            message=(
                                "Found different number of columns than types"
                                f" in {self}."
                            ),
                            context=str(self),
                        ),
                    )
                    break
            else:
                col = Column(type.name)

            col.add_type(type.type)
            self._columns.append(col)


@dataclass(eq=False)
class LateralView(Node):
    """
    Represents a lateral view expression
    """

    outer: bool = False
    func: FunctionTableExpression = field(default_factory=FunctionTableExpression)

    def __str__(self) -> str:
        parts = ["LATERAL VIEW"]
        if self.outer:
            parts.append(" OUTER")
        parts.append(f" {self.func}")
        return "".join(parts)


@dataclass(eq=False)
class Relation(Node):
    """
    Represents a relation
    """

    primary: Expression
    extensions: List[Join] = field(default_factory=list)

    def __str__(self) -> str:
        if self.extensions:
            extensions = " " + "\n".join([str(ext) for ext in self.extensions])
        else:
            extensions = ""
        return f"{self.primary}{extensions}"


@dataclass(eq=False)
class From(Node):
    """
    Represents the FROM clause of a SELECT statement
    """

    relations: List[Relation] = field(default_factory=list)

    def __str__(self) -> str:
        parts = ["FROM "]
        parts += ",\n".join([str(r) for r in self.relations])

        return "".join(parts)


@dataclass(eq=False)
class SetOp(Node):
    """
    A set operation
    """

    kind: str = ""  # Union, intersect, ...
    right: Optional["SelectExpression"] = None

    def __str__(self) -> str:
        return f"\n{self.kind}\n{self.right}"


@dataclass(eq=False)
class Cast(Expression):
    """
    A cast to a specified type
    """

    data_type: ColumnType
    expression: Expression

    def __str__(self) -> str:
        return f"CAST({self.expression} AS {str(self.data_type).upper()})"

    @property
    def type(self) -> ColumnType:
        """
        Return the type of the expression
        """
        return self.data_type

    async def compile(self, ctx: CompileContext):
        """
        In most cases we can short-circuit the CAST expression's compilation, since the output
        type is determined directly by `data_type`, so evaluating the expression is unnecessary.
        """
        for child in self.find_all(Column):
            await child.compile(ctx)

        if self.data_type:
            return


@dataclass(eq=False)
class SortItem(Node):
    """
    Defines a sort item of an expression
    """

    expr: Expression
    asc: str
    nulls: str

    def __str__(self) -> str:
        return f"{self.expr} {self.asc} {self.nulls}".strip()


@dataclass(eq=False)
class Organization(Node):
    """
    Sets up organization for the query
    """

    order: List[SortItem] = field(default_factory=list)
    sort: List[SortItem] = field(default_factory=list)

    def __str__(self) -> str:
        ret = ""
        ret += f"ORDER BY {', '.join(str(i) for i in self.order)}" if self.order else ""
        if ret:
            ret += "\n"
        ret += f"SORT BY {', '.join(str(i) for i in self.sort)}" if self.sort else ""
        return ret


@dataclass(eq=False)
class Hint(Node):
    """
    An Spark SQL hint statement
    """

    name: Name
    parameters: List[Column] = field(default_factory=list)

    def __str__(self) -> str:
        params = (
            f"({', '.join(str(param) for param in self.parameters)})"
            if self.parameters
            else ""
        )
        return f"{self.name}{params}"


@dataclass(eq=False)
class SelectExpression(Aliasable, Expression):
    """
    An uninitializable Type for Select for use as a default where
    a Select is required.
    """

    quantifier: str = ""  # Distinct, All
    projection: List[Union[Aliasable, Expression]] = field(default_factory=list)
    from_: Optional[From] = None
    group_by: List[Expression] = field(default_factory=list)
    having: Optional[Expression] = None
    where: Optional[Expression] = None
    lateral_views: List[LateralView] = field(default_factory=list)
    set_op: Optional[SetOp] = None
    limit: Optional[Expression] = None
    organization: Optional[Organization] = None
    hints: Optional[List[Hint]] = None

    def add_set_op(self, set_op: SetOp):
        if self.set_op:
            self.set_op.right.add_set_op(set_op)
        else:
            self.set_op = set_op

    def add_aliases_to_unnamed_columns(self) -> None:
        """
        Add an alias to any unnamed columns in the projection (`col{n}`)
        """
        projection = []
        for i, expression in enumerate(self.projection):
            if not isinstance(expression, Aliasable):
                name = f"col{i}"
                projection.append(expression.set_alias(Name(name)))
            else:
                projection.append(expression)
        self.projection = projection

    def where_clause_expressions_list(self) -> Optional[List[Expression]]:
        """
        Converts the WHERE clause to a list of expressions separated by AND operators
        """
        if not self.where:
            return self.where

        filters = []
        processing = collections.deque([self.where])
        while processing:
            current_clause = processing.pop()
            if current_clause:
                if (
                    isinstance(current_clause, BinaryOp)
                    and current_clause.op == BinaryOpKind.And
                ):
                    processing.append(current_clause.left)
                    processing.append(current_clause.right)
                else:
                    filters.append(current_clause)
        return filters

    @property
    def column_mapping(self) -> Dict[str, "Column"]:
        """
        Returns a dictionary with the output column names mapped to the columns
        """
        return {col.alias_or_name.name: col for col in self.projection}


class Select(SelectExpression):
    """
    A single select statement type
    """

    def __str__(self) -> str:
        parts = ["SELECT "]
        if self.hints:
            parts.append(f"/*+ {', '.join(str(hint) for hint in self.hints)} */\n")
        if self.quantifier:
            parts.append(f"{self.quantifier}\n")
        parts.append(",\n\t".join(str(exp) for exp in self.projection))
        if self.from_ is not None:
            parts.extend(("\n", str(self.from_), "\n"))
        for view in self.lateral_views:
            parts.append(f"\n{view}")
        if self.where is not None:
            parts.extend(("WHERE ", str(self.where), "\n"))
        if self.group_by:
            parts.extend(("GROUP BY ", ", ".join(str(exp) for exp in self.group_by)))
        if self.having is not None:
            parts.extend(("HAVING ", str(self.having), "\n"))
        select = " ".join(parts).strip()
        if self.parenthesized:
            select = f"({select})"
        if self.set_op:
            select += f"\n{self.set_op}"

        if self.organization:
            select += f"\n{self.organization}"
        if self.limit:
            select += f"\nLIMIT {self.limit}"

        if self.alias:
            as_ = " AS " if self.as_ else " "
            return f"{select}{as_}{self.alias}"
        if isinstance(self.parent, Alias):
            if self.set_op:
                return f"({select})"
        return select

    @property
    def type(self) -> ColumnType:
        if len(self.projection) != 1:
            raise DJParseException(
                "Can only infer type of a SELECT when it "
                f"has a single expression in its projection. In {self}.",
            )
        return self.projection[0].type

    async def compile(self, ctx: CompileContext):
        if not self.group_by and self.having:
            ctx.exception.errors.append(
                DJError(
                    code=ErrorCode.INVALID_SQL_QUERY,
                    message=(
                        "HAVING without a GROUP BY is not allowed. "
                        "Did you want to use a WHERE clause instead?"
                    ),
                    context=str(self),
                ),
            )
        await super().compile(ctx)


@dataclass(eq=False)
class Query(TableExpression, UnNamed):
    """
    Overarching query type
    """

    select: SelectExpression = field(default_factory=SelectExpression)
    ctes: List["Query"] = field(default_factory=list)

    def is_compiled(self) -> bool:
        return not any(
            self.filter(lambda node: node is not self and not node.is_compiled()),
        )

    async def compile(self, ctx: CompileContext):
        if self._is_compiled:
            return

        def _compile(info: Tuple[Column, List[TableExpression]]):
            """
            Given a list of table sources, find a matching origin table for the column.
            """
            col, table_options = info
            matching_origin_tables = 0
            for option in table_options:
                namespace = col.namespace[0].name if col.namespace else None
                table_alias = option.alias.name if option.alias else None
                if namespace is None or namespace == table_alias:
                    result = option.add_column_reference(col)
                    if result:
                        matching_origin_tables += 1
                        col._is_compiled = True
            if matching_origin_tables > 1:
                ctx.exception.errors.append(
                    DJError(
                        code=ErrorCode.INVALID_COLUMN,
                        message=f"Column `{col.name.name}` found in multiple tables."
                        " Consider using fully qualified name.",
                    ),
                )

        # Work backwards from the table expressions on the query's SELECT clause
        # and assign references between the columns and the tables
        nearest_query = self.get_nearest_parent_of_type(Query)
        cte_mapping = {
            cte.alias_or_name.name: cte
            for cte in (nearest_query.ctes if nearest_query else [])
        }
        table_options = (
            [
                tbl
                for tbl in self.select.from_.find_all(TableExpression)
                if tbl.get_nearest_parent_of_type(Query) is self
            ]
            if self.select.from_
            else []
        )
        if table_options:
            for idx, option in enumerate(table_options):
                if isinstance(option, Table):
                    if option.name.name in cte_mapping:
                        table_options[idx] = cte_mapping[option.name.name]
                await table_options[idx].compile(ctx)

            expressions_to_compile = [
                self.select.projection,
                self.select.group_by,
                self.select.having,
                self.select.where,
                self.select.organization,
            ]
            columns_to_compile = []
            for expression in expressions_to_compile:
                if expression and not isinstance(expression, list):
                    columns_to_compile += list(expression.find_all(Column))
                if isinstance(expression, list):
                    columns_to_compile += [
                        col for expr in expression for col in expr.find_all(Column)
                    ]

            with ThreadPoolExecutor() as executor:
                list(
                    executor.map(
                        _compile,
                        [(col, table_options) for col in columns_to_compile],
                    ),
                )

        for child in self.children:
            if child is not self and not child.is_compiled():
                await child.compile(ctx)

        for expr in self.select.projection:
            self._columns += expr.columns
        self._is_compiled = True

    def bake_ctes(self) -> "Query":
        """
        Add ctes into the select and return the select

        Note: This destroys the structure of the query which cannot be undone
        you may want to deepcopy it first
        """

        for cte in self.ctes:
            for tbl in self.filter(
                lambda node: isinstance(node, Table)
                and node.identifier(False) == cte.alias_or_name.identifier(False),
            ):
                tbl.swap(cte)
        self.ctes = []
        return self

    def to_cte(self, cte_name: Name, parent_ast: Optional["Query"] = None) -> "Query":
        """
        Prepares the query to be a CTE
        """
        self.alias = cte_name
        self.parenthesized = True
        self.as_ = True
        if parent_ast:
            self.set_parent(parent_ast, "ctes")
        return self

    def __str__(self) -> str:
        is_cte = self.parent is not None and self.parent_key == "ctes"
        ctes = ",\n".join(str(cte) for cte in self.ctes)
        if ctes:
            ctes += "\n\n"
        with_ = f"WITH\n{ctes}" if ctes else ""

        parts = [f"{with_}{self.select}\n"]
        query = "".join(parts)
        newline = "\n" if is_cte else ""
        if self.parenthesized:
            query = f"({newline}{query.strip()}{newline})"
        if self.alias:
            as_ = " AS " if self.as_ else " "
            if is_cte:
                query = f"{self.alias}{as_}{query}"
            else:
                query = f"{query}{as_}{self.alias}"
        return query

    def set_alias(self: TNode, alias: "Name") -> TNode:
        self.alias = alias
        for col in self._columns:
            if isinstance(col, Column) and col.table is not None:
                col.table.alias = self.alias
        return self

    async def extract_dependencies(
        self,
        context: Optional[CompileContext] = None,
    ) -> Tuple[Dict[NodeRevision, List[Table]], Dict[str, List[Table]]]:
        """
        Find all dependencies in a compiled query
        """

        if not self.is_compiled():
            if not context:
                raise DJException("Context not provided for query compilation!")
            await self.compile(context)

        deps: Dict[NodeRevision, List[Table]] = {}
        danglers: Dict[str, List[Table]] = {}
        for table in self.find_all(Table):
            if node := table.dj_node:
                deps[node] = deps.get(node, [])
                deps[node].append(table)
            else:
                name = table.identifier(quotes=False)
                danglers[name] = danglers.get(name, [])
                danglers[name].append(table)

        return deps, danglers

    @property
    def type(self) -> ColumnType:
        return self.select.type

    async def build(  # pylint: disable=R0913,C0415
        self,
        session: AsyncSession,
        memoized_queries: Dict[int, "Query"],
        build_criteria: Optional[BuildCriteria] = None,
        filters: Optional[List[str]] = None,
        dimensions: Optional[List[str]] = None,
        access_control=None,
    ):
        """
        Transforms a query ast by replacing dj node references with their asts
        """
        from datajunction_server.construction.build import _build_select_ast

        self.bake_ctes()  # pylint: disable=W0212
        await _build_select_ast(
            session,
            self.select,
            memoized_queries,
            build_criteria,
            filters,
            dimensions,
            access_control,
        )
        self.select.add_aliases_to_unnamed_columns()
