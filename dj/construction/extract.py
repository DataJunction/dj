"""
Functions for extracting DJ information from an AST
"""

from typing import Dict, List, Optional, Tuple

from sqlmodel import Session

from dj.construction.compile import CompoundBuildException, make_name
from dj.errors import DJException
from dj.models.node import NodeRevision, NodeType
from dj.sql.parsing import ast, parse


def extract_dependencies_from_query_ast(
    session: Session,
    query: ast.Query,
) -> Tuple[ast.Query, Dict[NodeRevision, List[ast.Table]], Dict[str, List[ast.Table]]]:
    """Find all dependencies in a query"""
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(False)
    query.compile(session)
    CompoundBuildException().reset()
    extractions = extract_dependencies_from_compiled_query_ast(query)
    return extractions


def extract_dependencies_from_compiled_query_ast(
    query: ast.Query,
) -> Tuple[ast.Query, Dict[NodeRevision, List[ast.Table]], Dict[str, List[ast.Table]]]:
    """Find all dependencies in a compiled query"""
    deps: Dict[NodeRevision, List[ast.Table]] = {}
    danglers: Dict[str, List[ast.Table]] = {}
    for table in query.find_all(ast.Table):
        if node := table.dj_node:
            deps[node] = deps.get(node, [])
            deps[node].append(table)
        else:
            name = make_name(table.namespace, table.name.name)
            danglers[name] = danglers.get(name, [])
            danglers[name].append(table)

    for col in query.find_all(ast.Column):
        if isinstance(col.table, ast.Table):
            if node := col.table.dj_node:  # pragma: no cover
                if node.type == NodeType.DIMENSION:
                    deps[node] = deps.get(node, [])
                    deps[node].append(col.table)

    return query, deps, danglers


def extract_dependencies_from_str_query(
    session: Session,
    query: str,
    dialect: Optional[str] = None,
) -> Tuple[ast.Query, Dict[NodeRevision, List[ast.Table]], Dict[str, List[ast.Table]]]:
    """Find all dependencies in the a string query"""
    return extract_dependencies_from_query_ast(session, parse(query, dialect))


def extract_dependencies_from_node(
    session: Session,
    node: NodeRevision,
    dialect: Optional[str] = None,
) -> Tuple[ast.Query, Dict[NodeRevision, List[ast.Table]], Dict[str, List[ast.Table]]]:
    """Find all immediate dependencies of a Node"""
    if node.query is None:
        raise DJException("Node has no query to extract from.")
    return extract_dependencies_from_str_query(session, node.query, dialect)
