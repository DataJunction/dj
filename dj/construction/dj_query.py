"""
Functions for making queries directly against DJ
"""

from typing import List, Optional, Set, cast

from sqlmodel import Session

from dj.construction.build import build_ast
from dj.construction.utils import amenable_name, get_dj_node, make_name
from dj.errors import DJException
from dj.models.node import NodeRevision, NodeType
from dj.sql.parsing import ast
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.sql.parsing.backends.sqloxide import parse


def try_get_dj_node(
    session: Session,
    name: str,
    kinds: Set[NodeType],
) -> Optional[NodeRevision]:
    "wraps get dj node to return None if no node is found"
    try:
        return get_dj_node(session, name, kinds)
    except DJException:
        return None


def _resolve_metric_nodes(session, col):
    """
    Check if a column is a metric and modify the
    select accordingly
    """
    joins = []
    col_name = make_name(col.namespace, col.name.name)
    if metric_node := try_get_dj_node(
        session,
        col_name,
        {NodeType.METRIC},
    ):
        # if we found a metric node we need to check where it came from
        parent_select = cast(ast.Select, col.get_nearest_parent_of_type(ast.Select))
        if not getattr(
            parent_select,
            "_validated",
            False,
        ):  # pragma: no cover# we haven't seen this
            if len(parent_select.from_.tables) != 1 or parent_select.from_.joins:
                raise DJParseException(
                    "Any SELECT referencing a Metric must source "
                    "from a single unaliased Table named `metrics`.",
                )
            metrics_ref = parent_select.from_.tables[0]
            try:
                metrics_ref_name = make_name(
                    metrics_ref.namespace,  # type: ignore
                    metrics_ref.name.name,  # type: ignore
                )
            except AttributeError:  # pragma: no cover
                metrics_ref_name = ""
            if metrics_ref_name != "metrics":
                raise DJParseException(
                    "The name of the table in a Metric query must be `metrics`.",
                )
            parent_select.from_ = ast.From(
                [],
            )  # clear the FROM to prep it for the actual tables
            parent_select._validated = True  # pylint: disable=W0212

        # we have a metric from `metrics`
        metric_name = amenable_name(metric_node.name)
        metric_select = parse(  # pylint: disable=W0212
            cast(str, metric_node.query),
        ).to_select()
        tables = metric_select.from_.tables + [
            join.table for join in metric_select.from_.joins
        ]
        metric_table_expression = ast.Alias(
            ast.Name(metric_name),
            None,
            metric_select,
        )

        for table in tables:
            joins += _hoist_metric_source_tables(
                session,
                table,
                metric_select,
                metric_table_expression,
            )

        metric_column = ast.Column(
            ast.Name(metric_node.columns[0].name),
            _table=metric_table_expression,
        )

        parent_select.replace(col, metric_column)
        parent_select.from_.tables = [metric_table_expression]
        parent_select.from_.joins += joins


def _hoist_metric_source_tables(
    session,
    table,
    metric_select,
    metric_table_expression,
) -> List[ast.Join]:
    """
    Hoist tables in a metric query
    we go through all the dep nodes directly in the metric's FROM
    we need to surface the node itself to join potential dims
    and to surface the node we need to source all its columns
    - in the metric for an implicit join
    """
    joins = []
    if isinstance(table, ast.Select):
        return []  # pragma: no cover
    if isinstance(table, ast.Alias):
        if isinstance(table.child, ast.Select):
            return []  # pragma: no cover
        table = table.child
    table_name = make_name(table.namespace, table.name.name)
    if table_node := try_get_dj_node(  # pragma: no cover
        session,
        table_name,
        {NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION},
    ):
        source_cols = []
        for tbl_col in table_node.columns:
            source_cols.append(_make_source_columns(tbl_col, table))
        # add the source's columns to the metric projection
        # so we can left join hoist the source alongside the metric select
        # so that dimensions can join properly in build
        metric_select.projection += source_cols
        # make the comparison expressions for the left join
        # that will hoist the source up
        ons = []
        for src_col in source_cols:
            ons.append(
                _source_column_join_on_expression(src_col, metric_table_expression),
            )
        # make the join
        if ons:  # pragma: no cover
            joins.append(
                ast.Join(
                    ast.JoinKind.LeftOuter,
                    table.alias_or_self().copy(),
                    on=ast.BinaryOp.And(*ons),  # type: ignore  # pylint: disable=no-value-for-parameter
                ),
            )
    return joins


def _make_source_columns(tbl_col, table) -> ast.Alias[ast.Column]:
    """
    Make the source columns for hoisting
    """
    temp_col = ast.Column(
        ast.Name(tbl_col.name),
        _table=table.alias_or_self(),
    )
    return ast.Alias(
        ast.Name(amenable_name(str(temp_col))),
        child=temp_col,
    )


def _source_column_join_on_expression(
    src_col,
    metric_table_expression,
) -> List[ast.BinaryOp]:
    """
    Make the part of the ON for the source column
    """
    return ast.BinaryOp.Eq(  # type: ignore
        ast.Column(
            ast.Name(src_col.name.name),
            _table=metric_table_expression,
        ),
        src_col.child.copy(),
    )


def _label_dimension_nodes(session, col):
    """
    Mark dimensions as api columns for compile to acknowledge them
    """
    col_name = make_name(col.namespace)
    if try_get_dj_node(session, col_name, {NodeType.DIMENSION}):
        col.set_api_column(True)


def build_dj_metric_query(  # pylint: disable=R0914,R0912
    session: Session,
    query: str,
    dialect: Optional[str] = None,
) -> ast.Query:
    """
    Build a dj query in SQL that may include dj metrics
    """
    query_ast = parse(query, dialect)
    select = query_ast.to_select()  # pylint: disable=W0212
    # we check all columns looking for metric nodes
    for col in select.find_all(ast.Column):
        _resolve_metric_nodes(session, col)

    # make the ast aware of all dimensions that are mentioned
    # that have come from the api
    for col in select.find_all(ast.Column):
        _label_dimension_nodes(session, col)

    return build_ast(
        session,
        query=ast.Query(select),
        build_criteria=None,
    )
