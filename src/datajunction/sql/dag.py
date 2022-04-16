"""
DAG related functions.
"""

from collections import defaultdict
from io import StringIO
from typing import Any, DefaultDict, Dict, List, Optional, Set

import asciidag.graph
import asciidag.node
from sqloxide import parse_sql

from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.sql.parse import find_nodes_by_key
from datajunction.typing import ParseTree


def render_dag(dependencies: Dict[str, Set[str]], **kwargs: Any) -> str:
    """
    Render the DAG of dependencies.
    """
    out = StringIO()
    graph = asciidag.graph.Graph(out, **kwargs)

    asciidag_nodes: Dict[str, asciidag.node.Node] = {}
    tips = sorted(
        [build_asciidag(name, dependencies, asciidag_nodes) for name in dependencies],
        key=lambda n: n.item,
    )

    graph.show_nodes(tips)
    out.seek(0)
    return out.getvalue()


def build_asciidag(
    name: str,
    dependencies: Dict[str, Set[str]],
    asciidag_nodes: Dict[str, asciidag.node.Node],
) -> asciidag.node.Node:
    """
    Build the nodes for ``asciidag``.
    """
    if name in asciidag_nodes:
        asciidag_node = asciidag_nodes[name]
    else:
        asciidag_node = asciidag.node.Node(name)
        asciidag_nodes[name] = asciidag_node

    asciidag_node.parents = sorted(
        [
            build_asciidag(child, dependencies, asciidag_nodes)
            for child in dependencies[name]
        ],
        key=lambda n: n.item,
    )

    return asciidag_node


def get_computable_databases(
    node: Node,
    columns: Optional[Set[str]] = None,
) -> Set[Database]:
    """
    Return all the databases where a given node can be computed.

    This takes into consideration the node expression, since some of the columns might
    not be present in all databases.
    """
    if columns is None:
        columns = {column.name for column in node.columns}

    # add all the databases where the node is explicitly materialized
    tables = [
        table
        for table in node.tables
        if columns <= {column.name for column in table.columns}
    ]
    databases = {table.database for table in tables}

    # add all the databases that are common between the parents and match all the columns
    parent_columns = get_referenced_columns_from_sql(node.expression, node.parents)
    if node.parents:
        parent_databases = [
            get_computable_databases(parent, parent_columns[parent.name])
            for parent in node.parents
        ]
        databases |= set.intersection(*parent_databases)

    return databases


def get_referenced_columns_from_sql(
    sql: Optional[str],
    parents: List[Node],
) -> DefaultDict[str, Set[str]]:
    """
    Given a SQL expression, return the referenced columns.

    Referenced columns are a dictionary mapping parent name to column name(s).
    """
    if not sql:
        return defaultdict(set)

    tree = parse_sql(sql, dialect="ansi")

    return get_referenced_columns_from_tree(tree, parents)


def get_referenced_columns_from_tree(
    tree: ParseTree,
    parents: List[Node],
) -> DefaultDict[str, Set[str]]:
    """
    Return the columns referenced in parents given a parse tree.
    """
    referenced_columns: DefaultDict[str, Set[str]] = defaultdict(set)

    parent_columns = {
        parent.name: {column.name for column in parent.columns} for parent in parents
    }

    # compound identifiers are fully qualified
    for compound_identifier in find_nodes_by_key(tree, "CompoundIdentifier"):
        parent = ".".join(part["value"] for part in compound_identifier[:-1])
        column = compound_identifier[-1]["value"]
        referenced_columns[parent].add(column)

    # for regular identifiers we need to figure out which parent the columns belongs to
    for identifier in find_nodes_by_key(tree, "Identifier"):
        column = identifier["value"]
        candidates = [
            parent for parent, columns in parent_columns.items() if column in columns
        ]
        if not candidates:
            raise Exception(f"Column {column} not found in any parent")
        if len(candidates) > 1:
            raise Exception(f"Column {column} is ambiguous")
        parent = candidates[0]
        referenced_columns[parent].add(column)

    return referenced_columns


def get_dimensions(node: Node) -> List[str]:
    """
    Return the available dimensions in a given node.
    """
    dimensions = []
    for parent in node.parents:
        for column in parent.columns:
            dimensions.append(f"{parent.name}.{column.name}")

            if column.dimension:
                for dimension_column in column.dimension.columns:
                    dimensions.append(
                        f"{column.dimension.name}.{dimension_column.name}",
                    )

    return sorted(dimensions)
