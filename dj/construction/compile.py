"""
Functions for transforming an AST using DJ information
"""

from itertools import chain
from typing import Dict, List, Optional, Set, Tuple, Union, cast

from sqlmodel import Session, select

from dj.construction.exceptions import CompoundBuildException
from dj.construction.utils import get_dj_node, make_name
from dj.errors import DJError, DJException, ErrorCode
from dj.models.node import Node, NodeType
import dj.sql.parsing.ast 
from dj.sql.parsing.backends.sqloxide import parse


def _check_col(
    col: ast.Column,
    table_nodes: Dict[str, ast.TableExpression],
    multiple_refs: Set[str],
    namespaces: Dict[str, Dict[str, Union[ast.Expression, ast.Column]]],
) -> Optional[str]:
    """
    Check if a column can be had in the query
    """
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
        if not namespace:  # pragma: no cover
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
        col.add_table(cast(ast.TableExpression, table_nodes[namespace].alias_or_self()))
        col_exp = namespaces[namespace][col.name.name]
        if isinstance(col_exp, ast.Expression):
            col.add_expression(col_exp)  # pragma: no cover
        else:
            col.add_type(col_exp.type)
    else:  # finally check if the column that does not have a namespace is in any namespace
        for nmpsc, nmspc_cols in namespaces.items():  # pragma: no cover

            if col.name.name in nmspc_cols:
                col.add_table(cast(ast.TableExpression, table_nodes[nmpsc].alias_or_self()))

                col_exp = namespaces[nmpsc][col.name.name]
                if isinstance(col_exp, ast.Expression):
                    col.add_expression(col_exp)
                else:
                    col.add_type(col_exp.type)
                return nmpsc
    return namespace


def _tables_to_namespaces(
    session: Session,
    namespaces: Dict[str, Dict[str, Union[ast.Expression, ast.Column]]],
    table: ast.TableExpression,
) -> Tuple[
    List[ast.Select],
    Dict[str, ast.TableExpression],
    Tuple[Set[Node], Set[Node], Set[Node]],
]:
    """
    Get all usable namespaces and columns from tables
    """

    # namespace: ast node defining namespace
    table_nodes: Dict[str, ast.TableExpression] = {}

    # track subqueries encountered to extract from them after
    subqueries: List[ast.Select] = []

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()
    sources_transforms: Set[Node] = set()
    dimensions_tables: Set[Node] = set()

    # get all usable namespaces and columns from the tables
    namespace = ""
    if isinstance(table, ast.Named):
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
        CompoundBuildException().append(  # pragma: no cover
            DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=f"Duplicate name `{namespace}` for table {table}",
            ),
            message="Cannot extract dependencies from SELECT",
        )

    namespaces[namespace] = {}

    if isinstance(table, ast.Alias):
        table: Union[Table, Select] = table.child  # type: ignore

    # subquery handling
    # we track subqueries separately and extract at the end
    # but introspect the columns to make sure the parent query selection is valid
    if isinstance(table, ast.Select):
        subqueries.append(table)

        for col in table.projection:
            if not isinstance(col, ast.Named):
                CompoundBuildException().append(  # pragma: no cover
                    error=DJError(
                        code=ErrorCode.INVALID_SQL_QUERY,
                        message=f"{col} is an unnamed expression. Try adding an alias.",
                        context=str(select),
                    ),
                    message="Cannot extract dependencies from SELECT.",
                )
            else:
                namespaces[namespace][col.name.name] = col
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
            namespaces[namespace].update({c.name: c for c in table_node.columns})
            if table_node.type in (NodeType.SOURCE, NodeType.TRANSFORM):
                sources_transforms.add(table_node)
            else:
                dimensions_tables.add(table_node)
            cast(ast.Table, table).add_dj_node(table_node)
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
    table_nodes: Dict[str, ast.TableExpression],
    multiple_refs: Set[str],
    namespaces: Dict[str, Dict[str, Union[ast.Expression, ast.Column]]],
) -> Set[Node]:
    """
    Check groupby, filters, and join ons columns for existence
    """

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()

    gbfo: List[Tuple[ast.Column, bool]] = []

    if select.group_by:
        gbfo += [
            (col, True)
            for col in chain(*(exp.find_all(ast.Column) for exp in select.group_by))
        ]
        if select.having:
            gbfo += [(col, True) for col in select.having.find_all(ast.Column)]
    elif select.having:
        CompoundBuildException().append(
            DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message=(
                    "HAVING without a GROUP BY is not allowed. "
                    "Did you want to use a WHERE clause instead?"
                ),
                context=str(select),
            ),
            message="Cannot extract dependencies from SELECT",
        )

    if select.where:
        gbfo += [(col, True) for col in select.where.find_all(ast.Column)]

    if select.from_.joins:
        for join in select.from_.joins:
            gbfo += [(col, False) for col in join.on.find_all(ast.Column)]

    for col, dim_allowed in gbfo:
        bad_namespace = False
        bad_col_exc = None
        try:
            namespace = _check_col(col, table_nodes, multiple_refs, namespaces)  # type: ignore
            bad_namespace = namespace is None
        except DJException as exc:
            bad_col_exc = exc
            bad_namespace = True
        if bad_namespace:
            namespace = make_name(col.namespace)
            if not dim_allowed:  # pragma: no cover
                dim = None
                try:  # see if the node is a dimension to inform an exception
                    dim = get_dj_node(session, namespace, {NodeType.DIMENSION})
                except DJException:
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
            else:  # dim allowed
                if bad_namespace:  # pragma: no cover
                    CompoundBuildException().errors = CompoundBuildException().errors[
                        :-1
                    ]
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
                        dim_table = ast.ast.Table(
                            col.namespace.names[0],  # type: ignore
                            Namespace(col.namespace.names[1:]),  # type: ignore
                        )
                        dim_table.add_dj_node(dim)
                        col.namespace = None
                        col.add_table(dim_table)
                        dimension_columns.add(dim)
    return dimension_columns


# flake8: noqa: C901
def compile_select(
    session: Session,
    select: ast.Select,  # pylint: disable= W0621
) -> ast.Select:
    """
    Get all dj node dependencies from a sql select while validating
    """
    # first, we get the tables in the from of the select including subqueries
    # we take stock of the columns that can come from said tables
    # then we check the select, groupby,
    # having/where for the columns keeping track of where they came from

    # depth 1 tables
    tables = select.from_.tables + [join.table for join in select.from_.joins]

    # namespaces track the namespace: list of columns that can be had from it
    namespaces: Dict[str, Dict[str, Union[ast.Expression, ast.Column]]] = {}

    # namespace: ast node defining namespace
    table_nodes: Dict[str, ast.TableExpression] = {}

    # track subqueries encountered to extract from them after
    subqueries: List[ast.Select] = []

    # used to check need and capacity for merging in dimensions
    dimension_columns: Set[Node] = set()
    sources_transforms: Set[Node] = set()
    dimensions_tables: Set[Node] = set()

    for table in tables:
        (
            _subqueries,
            _table_nodes,
            (_dimension_columns, _sources_transforms, _dimensions_tables),
        ) = _tables_to_namespaces(session, namespaces, table)
        subqueries += _subqueries
        table_nodes.update(_table_nodes)
        dimension_columns |= _dimension_columns
        sources_transforms |= _sources_transforms
        dimensions_tables |= _dimensions_tables

    # organize column discovery recording dupes
    # we'll use this lookup to validate columns
    no_namespace_safe_cols: Dict[str, Union[ast.Expression, ast.Column]] = {}
    multiple_refs: Set[str] = set()
    for namespaces_cols in namespaces.values():
        for col, exp_col in namespaces_cols.items():
            if col in no_namespace_safe_cols:
                multiple_refs.add(col)
            no_namespace_safe_cols[col] = exp_col

    namespaces[""] = namespaces.get("") or {}
    namespaces[""].update(
        {
            k: v
            for k, v in no_namespace_safe_cols.items()
            if k not in multiple_refs and k not in namespaces[""]
        },
    )

    # check projection
    for col in chain(*(exp.find_all(Column) for exp in select.projection)):  # type: ignore
        _check_col(
            cast(ast.Column, col),
            table_nodes,
            multiple_refs,
            namespaces,
        )

    dimension_columns |= _validate_groupby_filters_ons_columns(
        session,
        select,
        table_nodes,
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
        compile_select(session, subquery)

    return select


def compile_query(
    session: Session,
    query: ast.Query,
) -> ast.Query:
    """
    Get all dj node dependencies from a sql query while validating
    """
    # query = query.copy()
    select = query._to_select()
    compile_select(session, select)
    return query


def compile_node(session: Session, node: Node, dialect: Optional[str] = None) -> ast.Query:
    """
    Get all dj node dependencies from a sql query while validating
    """
    if node.query is None:
        raise DJException(f"Cannot compile node `{node.name}` with no query.")
    query = parse(node.query, dialect)
    return compile_query(session, query)
