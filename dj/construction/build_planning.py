"""
Functions for generating build plans for nodes
"""
from functools import reduce
from typing import Dict, List, Optional, Set, Tuple

from sqlmodel import Session

from dj.construction.extract import (
    extract_dependencies_from_node,
    extract_dependencies_from_query_ast,
)
from dj.models.database import Database
from dj.models.node import NodeRevision, NodeType
from dj.sql.dag import get_cheapest_online_database
from dj.sql.parsing import ast

BuildPlan = Tuple[ast.Query, Dict[NodeRevision, Tuple[Set[Database], "BuildPlan"]]]  # type: ignore


def get_materialized_databases_for_node(
    node: NodeRevision,
    columns: Set[str],
) -> Set[Database]:
    """
    Return all the databases where the node is explicitly materialized
    """
    tables = [
        table
        for table in node.tables
        if columns <= {column.name for column in table.columns}
    ]
    return {table.database for table in tables}


def generate_build_plan_from_query(
    session: Session,
    query: ast.Query,
    dialect: Optional[str] = None,
) -> BuildPlan:
    """
    creates a build plan to be followed by the building of a query

    processes nodes recursively extracting dependencies to
        build the BuildPlan which specifies an
            ast, node: {Databases, BuildPlan}
        that can be recursively followed as deep as desired to
        replace nodes with the desired ast
    """

    tree, deps, _ = extract_dependencies_from_query_ast(session, query)
    databases = {}
    for node, tables in deps.items():
        columns = {col.name.name for table in tables for col in table.columns}

        node_mat_dbs = get_materialized_databases_for_node(node, columns)
        build_plan = None
        if node.node.type != NodeType.SOURCE:
            build_plan = generate_build_plan_from_node(session, node, dialect)
        databases[node] = (node_mat_dbs, build_plan)

    return tree, databases


def generate_build_plan_from_node(
    session: Session,
    node: NodeRevision,
    dialect: Optional[str] = None,
) -> BuildPlan:
    """
    creates a build plan to be followed by the building of a node

    processes nodes recursively extracting dependencies to
        build the BuildPlan which specifies an
            ast, node: {Databases, BuildPlan}
        that can be recursively followed as deep as desired to
        replace nodes with the desired ast
    """
    if node.query is None:
        raise Exception(
            "Node has no query. Cannot generate a build plan without a query.",
        )
    tree, deps, _ = extract_dependencies_from_node(session, node, dialect)
    databases = {}
    for dependent_node, tables in deps.items():
        columns = {col.name.name for table in tables for col in table.columns}

        node_mat_dbs = get_materialized_databases_for_node(dependent_node, columns)
        build_plan = None
        if dependent_node.type != NodeType.SOURCE:
            build_plan = generate_build_plan_from_node(session, dependent_node, dialect)
        databases[dependent_node] = (node_mat_dbs, build_plan)

    return tree, databases


def _level_database(
    build_plan: BuildPlan,
    levels: List[List[Set[Database]]],
    level: int = 0,
    source_dbs: Optional[Set[Database]] = None,
):
    """
    takes a build plan and compounds each depth into levels
    """
    source_dbs = source_dbs or set()

    if levels is None:  # pragma: no cover
        levels = []
    sub_build_plan = build_plan[1]
    dbi = reduce(lambda a, b: a & b, (dbs for _, (dbs, _) in sub_build_plan.items()))
    if source_dbs:  # pragma: no cover
        dbi &= source_dbs

    while level >= len(levels):
        levels.append([])
    levels[level].append(dbi)

    for node, (sub_build_dbs, sub_sub_build_plan) in sub_build_plan.items():
        if node.node.type == NodeType.SOURCE:
            source_dbs.update(sub_build_dbs)
        if sub_sub_build_plan:
            _level_database(sub_sub_build_plan, levels, level + 1)


async def optimize_level_by_cost(build_plan: BuildPlan) -> Tuple[int, Database]:
    """
    from a build plan, determine how deep to follow the build plan
    by choosing the lowest cost database
    """
    levels: List[List[Set[Database]]] = []
    _level_database(build_plan, levels)
    some_db: bool = False
    cheapest_levels: List[Tuple[int, Database]] = []
    for i, level in enumerate(levels):
        combined_level = reduce(lambda a, b: a & b, level)
        if combined_level:
            try:
                cheapest_levels.append(
                    (i, await get_cheapest_online_database(combined_level)),
                )
                some_db = True
            except Exception as exc:  # pylint: disable=broad-except    # pragma: no cover
                if "No active database found" not in str(exc):
                    raise exc

    if not some_db:  # pragma: no cover
        raise Exception("No database found that can execute this query.")

    return sorted(
        cheapest_levels,
        key=lambda icl: icl[1].cost if icl[1] else float("-inf"),
    )[0]


async def optimize_level_by_database_id(
    build_plan: BuildPlan,
    database_id: int,
) -> Tuple[int, Database]:
    """
    from a build plan, determine how deep to follow the build plan
    by selecting the first level that can run completely in that database
    """

    levels: List[List[Set[Database]]] = []
    _level_database(build_plan, levels)
    combined_levels = [reduce(lambda a, b: a & b, level) for level in levels]
    for i, level in enumerate(combined_levels):
        for database in level:
            if database.id == database_id and await database.do_ping():
                return i, database
    raise Exception(
        f"The requested database with id {database_id} cannot run this query.",
    )
