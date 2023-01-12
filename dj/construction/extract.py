"""
Functions for extracting DJ information from nodes or SQL for validation and building.
"""

from dataclasses import dataclass, field
from itertools import chain
from typing import Dict, Generator, List, Optional, Set, Tuple, Union, cast

from sqlalchemy.orm.exc import NoResultFound
from sqlmodel import Session, select

from dj.errors import DJError, DJException, ErrorCode
from dj.models.node import Node, NodeType
from dj.sql.parsing.ast import (
    Alias,
    Column,
    Named,
    Namespace,
    Query,
    Select,
    Table,
    TableExpression,
)
from dj.sql.parsing.backends.sqloxide import parse


def make_name(namespace: Optional[Namespace], name="") -> str:
    """utility taking a namespace and name to make a possible name of a DJ Node"""
    ret = ""
    if namespace:
        ret += ".".join(name.name for name in namespace.names)
    if name:
        ret += ("." if ret else "") + name
    return ret


class CompoundBuildException:
    """
    Exception singleton to optionally build up exceptions or raise
    """

    errors: List[DJError]
    _instance: Optional["CompoundBuildException"] = None
    _raise: bool = True

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CompoundBuildException, cls).__new__(
                cls, *args, **kwargs
            )
            cls.errors = []
        return cls._instance

    def reset(self):
        """
        Resets the singleton
        """
        self._raise = True
        self.errors = []

    def clear(self):
        """
        Erases stored errors
        """
        self.errors = []

    def set_raise(self, raise_: bool):
        """
        Set whether to raise caught exceptions or accumulate them
        """
        self._raise = raise_

    def append(self, error: DJError, message: Optional[str]):
        """
        Accumulate DJ exceptions
        """
        if self._raise:
            raise DJException(
                message=message or error.message,
                errors=[error],
            )
        self.errors.append(error)

    def __str__(self) -> str:
        plural = "s" if len(self.errors) > 1 else ""
        error = f"Found {len(self.errors)} issue{plural}:\n"
        return error + "\n\n".join(
            "\t" + str(type(exc).__name__) + ": " + str(exc) + "\n" + "=" * 50
            for exc in self.errors
        )


def get_dj_node(
    session: Session,
    node_name: str,
    kinds: Optional[Set[NodeType]] = None,
) -> Optional[Node]:
    """Return the DJ Node with a given name from a set of node types"""
    query = select(Node).filter(Node.name == node_name)
    match = None
    try:
        match = session.exec(query).one()
    except NoResultFound:
        kind_msg = " or ".join(str(k) for k in kinds) if kinds else ""
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.UNKNOWN_NODE,
                message=f"No node `{node_name}` exists of kind {kind_msg}.",
            ),
            message=f"Cannot get DJ node {node_name}",
        )

    if match and kinds and (match.type not in kinds):
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.NODE_TYPE_ERROR,
                message=(
                    f"Node `{match.name}` is of type `{str(match.type).upper()}`. "
                    "Expected kind to be of {' or '.join(str(k) for k in kinds)}."
                ),
            ),
            message=f"Cannot get DJ node {node_name}",
        )

    return match


@dataclass
class ColumnDependencies:
    """Columns discovered from a query"""

    projection: List[Tuple[Column, TableExpression]] = field(
        default_factory=list,
    )  # selected nodes
    group_by: List[Tuple[Column, Union[TableExpression, Node]]] = field(
        default_factory=list,
    )
    filters: List[Tuple[Column, Union[TableExpression, Node]]] = field(
        default_factory=list,
    )  # where/having
    ons: List[Tuple[Column, Union[TableExpression, Node]]] = field(
        default_factory=list,
    )  # join ons

    @property
    def all_columns(
        self,
    ) -> Generator[Tuple[Column, Union[TableExpression, Node]], None, None]:
        """get all column, node pairs"""
        for pair in chain(
            iter(self.projection),
            iter(self.group_by),
            iter(self.filters),
            iter(self.ons),
        ):
            yield pair


@dataclass
class SelectDependencies:
    """stores all the dependencies found in a select statement"""

    tables: List[Tuple[TableExpression, Node]] = field(default_factory=list)
    columns: ColumnDependencies = field(default_factory=ColumnDependencies)
    subqueries: List[Tuple[Select, "SelectDependencies"]] = field(default_factory=list)

    @property
    def all_tables(self) -> Generator[Tuple[TableExpression, Node], None, None]:
        "get all table node, dj node pairs"
        for table in self.tables:
            yield table
        for _, subquery in self.subqueries:
            for table in subquery.all_tables:
                yield table

    @property
    def all_node_dependencies(self) -> Set[Node]:
        """get all dj nodes referenced"""
        return {node for _, node in self.all_tables if isinstance(node, Node)} | {
            node for _, node in self.columns.all_columns if isinstance(node, Node)
        }


@dataclass
class QueryDependencies:
    """stores all dependencies found in a query statement"""

    ctes: List[SelectDependencies] = field(default_factory=list)
    select: SelectDependencies = field(default_factory=SelectDependencies)

    @property
    def all_node_dependencies(self) -> Set[Node]:
        """get all dj nodes referenced"""
        ret = self.select.all_node_dependencies
        return ret


def _check_col(
    col: Column,
    table_nodes: Dict[str, TableExpression],
    add: List[Tuple[Column, Union[TableExpression, Node]]],
    multiple_refs: Set[str],
    namespaces: Dict[str, Set[str]],
) -> Optional[str]:
    """Check if a column can be had in the query"""
    namespace = make_name(col.namespace)  # str preceding the column name
    cols = namespaces.get(namespace)
    if cols is None:  # there is just no namespace at all where the node could come from
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.MISSING_COLUMNS,
                message=(
                    f"No namespace `{namespace}` from which to reference column "
                    f"`{col.name.name}`."
                ),
                context=str(col.parent),
            ),
            message="Cannot extract dependencies from SELECT.",
        )
        return None
    if (
        col.name.name not in cols
    ):  # the proposed namespace does not contain the column; which error to raise?
        exc_msg = f"Namespace `{namespace}` has no column `{col.name.name}`."
        error_code = ErrorCode.MISSING_COLUMNS
        if not namespace:
            exc_msg = (
                f"Column `{col.name.name}` does not exist in any referenced tables."
            )
            if col.name.name in multiple_refs:
                exc_msg = f"`{col.name.name}` appears in multiple references and so must be namespaced."  # pylint: disable=C0301
                error_code = ErrorCode.INVALID_SQL_QUERY
        CompoundBuildException().append(
            error=DJError(
                code=error_code,
                message=exc_msg,
                context=str(col.parent),
            ),
            message="Cannot extract dependencies from SELECT",
        )

        return None
    if namespace:  # there is a proposed namespace that has the column
        add.append((col, table_nodes[namespace]))
    else:  # finally check if the column that does not have a namespace is in any namespace
        for nmpsc, nmspc_cols in namespaces.items():  # pragma: no cover
            if col.name.name in nmspc_cols:
                add.append((col, table_nodes[nmpsc]))
                return namespace
    return namespace


def _tables_to_namespaces(
    session: Session,
    namespaces: Dict[str, Set[str]],
    table: TableExpression,
    table_deps: SelectDependencies,
) -> Tuple[
    List[Select],
    Dict[str, TableExpression],
    Tuple[Set[Node], Set[Node], Set[Node]],
]:
    """get all usable namespaces and columns from tables"""

    # namespace: ast node defining namespace
    table_nodes: Dict[str, TableExpression] = {}

    # track subqueries encountered to extract from them after
    subqueries: List[Select] = []

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()
    sources_transforms: Set[Node] = set()
    dimensions_tables: Set[Node] = set()

    # get all usable namespaces and columns from the tables
    namespace = ""
    if isinstance(table, Named):
        namespace = make_name(table.namespace, table.name.name)

    # you cannot combine an unnamed subquery with anything else
    if (namespace and "" in namespaces) or (namespace == "" and namespaces):
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=f"You may only use an unnamed subquery alone for {table}",
            ),
            message="Cannot extract dependencies from SELECT",
        )
    # you cannot have multiple references with the same name
    if namespace in namespaces:
        CompoundBuildException().append(
            DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=f"Duplicate name `{namespace}` for table {table}",
            ),
            message="Cannot extract dependencies from SELECT",
        )

    namespaces[namespace] = set()

    if isinstance(table, Alias):
        table: Union[Table, Select] = table.child  # type: ignore

    # subquery handling
    # we track subqueries separately and extract at the end
    # but introspect the columns to make sure the parent query selection is valid
    if isinstance(table, Select):
        subqueries.append(table)

        for col in table.projection:
            if not isinstance(col, Named):
                CompoundBuildException().append(
                    error=DJError(
                        code=ErrorCode.INVALID_SQL_QUERY,
                        message=f"{col} is an unnamed expression. Try adding an alias.",
                        context=str(select),
                    ),
                    message="Cannot extract dependencies from SELECT.",
                )
            else:
                namespaces[namespace].add(col.name.name)
    # tables are sought as nodes and nothing else
    # can be source, transform, dimension
    else:  # not select then is table
        node_name = make_name(table.namespace, table.name.name)
        table_node = get_dj_node(
            session,
            node_name,
            {NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION},
        )
        if table_node is not None:
            namespaces[namespace] |= {c.name for c in table_node.columns}
            table_deps.tables.append((table, table_node))
            if table_node.type in {NodeType.SOURCE, NodeType.TRANSFORM}:
                sources_transforms.add(table_node)
    table_nodes[namespace] = table

    return (
        subqueries,
        table_nodes,
        (dimension_columns, sources_transforms, dimensions_tables),
    )


# pylint: disable=R0914, R0913, R0912, W0621
def _validate_groupby_filters_ons_columns(
    session: Session,
    select: Select,
    table_nodes: Dict[str, TableExpression],
    table_deps: SelectDependencies,
    multiple_refs: Set[str],
    namespaces: Dict[str, Set[str]],
) -> Set[Node]:
    """check groupby, filters, and join ons columns for existence"""

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()

    gbfo: List[
        Tuple[List[Tuple[Column, Union[TableExpression, Node]]], Column, bool]
    ] = []

    if select.group_by:
        gbfo += [
            (table_deps.columns.group_by, col, True)
            for col in chain(*(exp.find_all(Column) for exp in select.group_by))
        ]
        if select.having:
            gbfo += [
                (table_deps.columns.filters, col, True)
                for col in select.having.find_all(Column)
            ]
    elif select.having:
        CompoundBuildException().append(
            DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message="HAVING without a GROUP BY is not allowed. Did you want to use WHERE clause instead?",  # pylint: disable=C0301
                context=str(select),
            ),
            message="Cannot extract dependencies from SELECT",
        )

    if select.where:
        gbfo += [
            (table_deps.columns.filters, col, True)
            for col in select.where.find_all(Column)
        ]

    if select.from_.joins:
        for join in select.from_.joins:
            gbfo += [
                (table_deps.columns.ons, col, False) for col in join.on.find_all(Column)
            ]

    for add, col, dim_allowed in gbfo:
        bad_namespace = False
        bad_col_exc = None
        try:
            namespace = _check_col(col, table_nodes, add, multiple_refs, namespaces)  # type: ignore
            bad_namespace = namespace is None
        except DJException as exc:
            bad_col_exc = exc
            bad_namespace = True
        if bad_namespace:
            namespace = make_name(col.namespace)
            if not dim_allowed:
                dim = None
                try:
                    dim = get_dj_node(session, namespace, {NodeType.DIMENSION})
                except DJException:  # pragma: no cover
                    pass
                CompoundBuildException().append(
                    error=DJError(
                        code=ErrorCode.INVALID_SQL_QUERY,
                        message=f"Cannot reference a dimension with {col.name}",
                        context=str(col.parent),
                    ),
                    message="Cannot extract dependencies from SELECT",
                )
                if bad_col_exc is not None:  # pragma: no cover
                    CompoundBuildException().append(
                        error=DJError(
                            code=ErrorCode.INVALID_COLUMN,
                            message=f"Invalid column in query {col.name}",
                        ),
                        message="Cannot extract dependencies from SELECT",
                    )
            else:

                dim = get_dj_node(session, namespace, {NodeType.DIMENSION})
                if dim is not None:  # pragma: no cover
                    if col.name.name not in {c.name for c in dim.columns}:
                        CompoundBuildException().append(
                            error=DJError(
                                code=ErrorCode.MISSING_COLUMNS,
                                message=(
                                    f"Dimension `{dim.name}` has no column "
                                    "`{col.name.name}`.",
                                ),
                                context=str(col.parent),
                            ),
                            message="Cannot extract dependencies from SELECT",
                        )
                    else:
                        add.append((col, dim))
                        dimension_columns.add(dim)
    return dimension_columns


# flake8: noqa: C901
def extract_dependencies_from_select(
    session: Session,
    select: Select,  # pylint: disable= W0621
) -> SelectDependencies:
    """get all dj node dependencies from a sql select while validating"""
    # first, we get the tables in the from of the select including subqueries
    # we take stock of the columns that can come from said tables
    # then we check the select, groupby,
    # having/where for the columns keeping track of where they came from

    table_deps = SelectDependencies()

    # depth 1 tables
    tables = select.from_.tables + [join.table for join in select.from_.joins]

    # namespaces track the namespace: list of columns that can be had from it
    namespaces: Dict[str, Set[str]] = {}

    # namespace: ast node defining namespace
    table_nodes: Dict[str, TableExpression] = {}

    # track subqueries encountered to extract from them after
    subqueries: List[Select] = []

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()
    sources_transforms: Set[Node] = set()
    dimensions_tables: Set[Node] = set()

    for table in tables:
        (
            _subqueries,
            _table_nodes,
            (_dimension_columns, _sources_transforms, _dimensions_tables),
        ) = _tables_to_namespaces(session, namespaces, table, table_deps)
        subqueries += _subqueries
        table_nodes.update(_table_nodes)
        dimension_columns |= _dimension_columns
        sources_transforms |= _sources_transforms
        dimensions_tables |= _dimensions_tables

    # organize column discovery recording dupes
    # we'll use this lookup to validate columns
    no_namespace_safe_cols: Set[str] = set()
    multiple_refs: Set[str] = set()
    for namespaces_cols in namespaces.values():
        for col in namespaces_cols:
            if col in no_namespace_safe_cols:
                multiple_refs.add(col)
            no_namespace_safe_cols.add(col)

    namespaces[""] = no_namespace_safe_cols - multiple_refs  # type: ignore

    # check projection
    for col in chain(*(exp.find_all(Column) for exp in select.projection)):  # type: ignore
        _check_col(
            cast(Column, col),
            table_nodes,
            table_deps.columns.projection,
            multiple_refs,
            namespaces,
        )

    dimension_columns |= _validate_groupby_filters_ons_columns(
        session,
        select,
        table_nodes,
        table_deps,
        multiple_refs,
        namespaces,
    )

    # check if there are any column dimension dependencies we need to join but cannot
    # if a dimension is already used directly in the from (manually join or ref'd) -
    # - there is no need to join it so we check only dimensions not used that way
    for dim in dimension_columns - dimensions_tables:
        joinable = False
        # it is not possible to have a dimension referenced
        # somewhere without some from tables
        for src_fm in sources_transforms | dimensions_tables:  # pragma: no cover
            for col in src_fm.columns:  # pragma: no cover
                if col.dimension == dim:  # pragma: no cover
                    joinable = True
                    break
            if joinable:
                break
        if not joinable:
            CompoundBuildException().append(
                error=DJError(
                    code=ErrorCode.INVALID_DIMENSION_JOIN,
                    message=(
                        f"Dimension `{dim.name}` is not joinable. A SOURCE, "
                        "TRANSFORM, or DIMENSION node which references this "
                        "dimension must be used directly in the FROM clause."
                    ),
                    context=str(select.from_),
                ),
                message="Cannot extract dependencies from SELECT",
            )

    for subquery in subqueries:
        table_deps.subqueries.append(
            (
                subquery,
                extract_dependencies_from_select(session, subquery),
            ),
        )

    return table_deps


def extract_dependencies_from_query(
    session: Session,
    query: Query,
) -> QueryDependencies:
    """get all dj node dependencies from a sql query while validating"""
    return QueryDependencies(
        select=extract_dependencies_from_select(session, query.select),
    )


def _extract_dependencies(
    session: Session,
    node: Node,
    dialect: Optional[str] = None,
    distance: int = -1,
) -> Tuple[Set[Node], Set[str]]:
    """Helper for extract_dependencies"""
    _distance = float("inf") if distance < 0 else float(distance)
    if node.query is None:
        raise Exception("Node has no query")
    ast = parse(node.query, dialect)
    deps: QueryDependencies = extract_dependencies_from_query(session, ast)
    dep_nodes: Set[Node] = deps.all_node_dependencies
    bad_dep_nodes: Set[str] = set()
    added = True
    travelled = 0
    new_dep_nodes: Set[Node] = set()
    new_bad_dep_nodes: Set[str] = set()
    while added and travelled < _distance:
        for dep_node in dep_nodes:
            if dep_node.type != NodeType.SOURCE:
                extract_dep_nodes, extract_bad_dep_nodes = _extract_dependencies(
                    session,
                    dep_node,
                    dialect,
                )
                new_dep_nodes |= extract_dep_nodes
                new_bad_dep_nodes |= extract_bad_dep_nodes
        curr_len = len(dep_nodes) + len(bad_dep_nodes)
        dep_nodes |= new_dep_nodes
        bad_dep_nodes |= new_bad_dep_nodes
        added = curr_len != (len(dep_nodes) + len(bad_dep_nodes))
        travelled += 1

    for err in CompoundBuildException().errors:
        if err.code in (ErrorCode.UNKNOWN_NODE, ErrorCode.NODE_TYPE_ERROR):
            bad_dep_nodes.add(err.context)

    return dep_nodes, bad_dep_nodes


def extract_dependencies(
    session: Session,
    node: Node,
    dialect: Optional[str] = None,
    raise_: bool = True,
    distance: int = -1,
) -> Tuple[Set[Node], Set[str]]:
    """Find all dependencies in the dj dag of a node

    distance: how many steps away to explore.
        <0 infinitely far,
        0 only neighbors e.g. only nodes referenced directly in the node query
    """
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(False)

    dep_nodes, bad_dep_nodes = _extract_dependencies(
        session=session, node=node, dialect=dialect, distance=distance,
    )

    if CompoundBuildException().errors and raise_:
        raise DJException(
            message=f"Cannot extract dependencies from node `{node.name}`",
            errors=CompoundBuildException().errors,
        )

    return dep_nodes, bad_dep_nodes
