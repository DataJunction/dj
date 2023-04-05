"""
Functions for making queries directly against DJ
"""

from typing import List, Optional, Set, cast

from sqlmodel import Session

from dj.construction.build import build_ast
from dj.construction.utils import amenable_name, get_dj_node
from dj.errors import DJErrorException
from dj.models.node import NodeRevision, NodeType
from dj.sql.parsing.backends.antlr4 import ast, parse
from dj.sql.parsing.backends.exceptions import DJParseException


def try_get_dj_node(
    session: Session,
    name: str,
    kinds: Set[NodeType],
) -> Optional[NodeRevision]:
    "wraps get dj node to return None if no node is found"
    try:
        return get_dj_node(session, name, kinds)
    except DJErrorException:
        return None


def _resolve_metric_nodes(session, col):
    """
    Check if a column is a metric and modify the
    select accordingly
    """
    joins = []
    col_name = col.identifier(False)
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
        ):  # pragma: no cover
            if (
                len(parent_select.from_.relations) != 1
                or parent_select.from_.relations[0].primary.alias_or_name.name
                != "metrics"
            ):
                raise DJParseException(
                    "Any SELECT referencing a Metric must source "
                    "from a single unaliased Table named `metrics`.",
                )
            metrics_ref = parent_select.from_.relations[0].primary
            try:
                metrics_ref_name = metrics_ref.alias_or_name.identifier(False)
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
        ).select
        tables = metric_select.from_.find_all(ast.Table)
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
            as_=True,
        )

        metric_table_expression.child.parenthesized = True
        parent_select.replace(col, metric_column)
        parent_select.from_.relations = [
            ast.Relation(primary=metric_table_expression.child, extensions=joins),
        ]


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
        if isinstance(table.child, ast.Select):  # pragma: no cover
            return []  # pragma: no cover
        table = table.child  # pragma: no cover
    table_name = table.identifier(False)
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
                    join_type="LEFT OUTER",
                    right=table.copy(),
                    criteria=ast.JoinCriteria(on=ast.BinaryOp.And(*ons)),  # type: ignore  # pylint: disable=no-value-for-parameter
                ),
            )
    return joins


def _make_source_columns(tbl_col, table) -> ast.Alias[ast.Column]:
    """
    Make the source columns for hoisting
    """
    temp_col = ast.Column(
        ast.Name(tbl_col.name),
        _table=table,
        as_=True,
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
            src_col.alias_or_name,
            _table=metric_table_expression,
        ),
        src_col.child.copy(),
    )


def build_dj_metric_query(  # pylint: disable=R0914,R0912
    session: Session,
    query: str,
    dialect: Optional[str] = None,  # pylint: disable=unused-argument
) -> ast.Query:
    """
    Build a dj query in SQL that may include dj metrics
    """
    query_ast = parse(query)
    select = query_ast.select
    # we check all columns looking for metric nodes
    for col in select.find_all(ast.Column):
        _resolve_metric_nodes(session, col)

    return build_ast(
        session,
        query=ast.Query(select=select),
        build_criteria=None,
    )
