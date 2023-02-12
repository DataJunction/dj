"""
Functions for making queries directly against DJ
"""

from dj.sql.parsing.backends.sqloxide import parse
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.construction.utils import make_name, get_dj_node
from dj.construction.build import build_ast_for_database
from dj.sql.parsing import ast
from typing import Optional, List, Tuple
from sqlmodel import Session
from dj.models.node import NodeType


def dj_query(session: Session, query: str, dialect: Optional[str] = None, database_id: Optional[int] = None):
    query_ast = parse(query, dialect)
    select = query_ast._to_select()
    if len(select.from_.tables)!=1 or select.from_.joins:
        raise DJParseException("DJ queries expect no joins and a single table in FROM.")
    source = select.from_.tables[0]
    if not isinstance(source, ast.Table):
        raise DJParseException("The DJ query source must be a single unaliased Table.")
    source_name = make_name(source.namespace, source.name.name)
    if source_name=='metrics':
        if not select.projection or len(select.projection)>1:
            raise DJParseException("A DJ metric query can only contain a single column expression.")

        metrics = list(select.projection[0].find_all(ast.Column))
        if len(metrics)!=1:
            raise DJParseException(f"A DJ metric query must contain a single column reference in the projection but saw {', '.join(str(metric) for metric in metrics)}.")
        metric = metrics[0]
        metric_name = make_name(metric.namespace, metric.name.name)
        metric_node = get_dj_node(session, metric_name, {NodeType.METRIC})
        metric_ast = parse(metric_node.query, dialect)#type: ignore
        metric_ast.select.group_by+=query_ast.select.group_by
        if metric_ast.select.where and select.where:
            metric_ast.select.where = ast.BinaryOp(ast.BinaryOpKind.And, metric_ast.select.where, select.where)
        elif select.where:
            metric_ast.select.where = select.where
        if metric_ast.select.having and select.having:
            metric_ast.select.having = ast.BinaryOp(ast.BinaryOpKind.And, metric_ast.select.having, select.having)
        elif select.having:
            metric_ast.select.having = select.having
        metric_ast.select.group_by += select.group_by
        select.where = None
        select.having = None
        select.group_by = []
        select.projection = []


        

    raise DJParseException(f"Query `{query}` is not an acceptable DJ query.")

