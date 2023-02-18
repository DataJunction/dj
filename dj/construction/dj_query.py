"""
Functions for making queries directly against DJ
"""

from typing import Optional, Tuple, cast

from sqlmodel import Session

from dj.construction.build import build_ast_for_database
from dj.construction.utils import amenable_name, get_dj_node, make_name
from dj.models.database import Database
from dj.models.node import NodeType
from dj.sql.parsing import ast
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.sql.parsing.backends.sqloxide import parse


async def build_dj_metric_query(  # pylint: disable=R0914,R0912
    session: Session,
    query: str,
    dialect: Optional[str] = None,
    database_id: Optional[int] = None,
) -> Tuple[ast.Query, Database]:
    """
    Build a dj query in SQL that may include dj metrics
    """
    query_ast = parse(query, dialect)
    select = query_ast._to_select()  # pylint: disable=W0212
    # we check all columns looking for metric nodes
    for col in select.find_all(ast.Column):
        froms = []
        col_name = make_name(col.namespace, col.name.name)
        if metric_node := get_dj_node(
            session,
            col_name,
            {NodeType.METRIC},
            raise_=False,
        ):
            # if we found a metric node we need to check where it came from
            parent_select = cast(ast.Select, col.get_nearest_parent_of_type(ast.Select))
            if not getattr(parent_select, "_validated", False):# we haven't seen this
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
                except AttributeError:
                    metrics_ref_name = ""
                if metrics_ref_name != "metrics":
                    raise DJParseException(
                        "The name of the table in a Metric query must be `metrics`.",
                    )
                parent_select.from_ = ast.From([])# clear the FROM to prep it for the actual tables
                parent_select._validated = True  # pylint: disable=W0212

            # we have a metric from `metrics`
            metric_name = amenable_name(metric_node.name)
            metric_select = parse(  # pylint: disable=W0212
                cast(str, metric_node.query),
            )._to_select()
            tables = metric_select.from_.tables + [
                join.table for join in metric_select.from_.joins
            ]
            # we go through all the dep nodes directly in the metric's from
            # we need to surface the node itself to join potential dims
            # and to surface the node we need to source all its columns
            # - in the metric for an implicit join
            for table in tables:
                if isinstance(table, ast.Select):
                    continue#pragma: no cover
                if isinstance(table, ast.Alias):
                    if isinstance(table.child, ast.Select):
                        continue
                    table = table.child
                table_name = make_name(table.namespace, table.name.name)
                if table_node := get_dj_node(
                    session,
                    table_name,
                    {NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION},
                    raise_=False,
                ):
                    metric_select.projection += [
                        ast.Column(ast.Name(col.name), _table = table.alias_or_self()) for col in table_node.columns
                    ]
                    froms.append(table.copy())
            metric_table_expression = ast.Alias(
                ast.Name(metric_name),
                None,
                metric_select,
            )
            froms.append(metric_table_expression)  # type: ignore
            metric_column = ast.Column(
                ast.Name(metric_node.columns[0].name),
                _table=metric_table_expression,
            )
            parent_select.replace(col, metric_column)
            parent_select.from_.tables += froms

    # make the ast aware of all dimensions that are mentioned
    # that have come from the api
    for col in select.find_all(ast.Column):
        col_name = make_name(col.namespace)
        if get_dj_node(session, col_name, {NodeType.DIMENSION}, raise_=False):
            col.set_api_column(True)

    return await build_ast_for_database(
        session,
        query=ast.Query(select),
        dialect=dialect,
        database_id=database_id,
    )
