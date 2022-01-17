"""
Functions for transpiling SQL.

These functions parse the DJ SQL used to define node expressions, and generate SQLAlchemy
queries which can be then executed in specific databases.
"""

from operator import attrgetter
from typing import Any, List, Optional

from sqlalchemy.engine import create_engine
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import Select, func, select
from sqloxide import parse_sql

from datajunction.models.node import Node
from datajunction.sql.parse import find_nodes_by_key


def get_query_for_node(node: Node) -> Select:
    """
    Build a SQLAlchemy ``select()`` for a given node.
    """
    # if the node is materialized we use the table with the cheapest cost
    if node.tables:
        table = sorted(node.tables, key=attrgetter("cost"))[0]
        engine = create_engine(table.database.URI)
        materialized_table = Table(
            table.table,
            MetaData(bind=engine),
            schema=table.schema_,
            autoload=True,
        )
        return select(materialized_table)

    tree = parse_sql(node.expression, dialect="ansi")

    projection = get_projection(node, tree)
    source = get_source(node, tree)
    return projection.select_from(source)


def get_projection(node: Node, tree: Any) -> Select:
    """
    Build the ``SELECT`` part of a query.
    """
    expressions = []
    projection = next(find_nodes_by_key(tree, "projection"))
    for expression in projection:
        alias: Optional[str] = None
        if "UnnamedExpr" in expression:
            expression = expression["UnnamedExpr"]
        elif "ExprWithAlias" in expression:
            alias = expression["ExprWithAlias"]["alias"]["value"]
            expression = expression["ExprWithAlias"]["expr"]
        else:
            raise NotImplementedError(f"Unable to handle expression: {expression}")

        expressions.append(get_expression(node.parents, expression, alias))

    return select(expressions)


def get_expression(
    parents: List[Node],
    expression: Any,
    alias: Optional[str] = None,
) -> Any:
    """
    Build an expression.
    """
    if "Function" in expression:
        return get_function(parents, expression["Function"], alias)
    if expression == "Wildcard":
        return "*"
    raise NotImplementedError(f"Unable to handle expression: {expression}")


def get_function(
    parents: List[Node],
    expression: Any,
    alias: Optional[str] = None,
) -> Any:
    """
    Build a function.
    """
    name = expression["name"][0]["value"]
    args = expression["args"]
    evaluated_args = [get_expression(parents, arg["Unnamed"]) for arg in args]
    return getattr(func, name.lower())(*evaluated_args).label(alias)


def get_source(node: Node, tree: Any) -> Select:  # pylint: disable=unused-argument
    """
    Build the ``FROM`` part of a query.
    """
    # For now assume no JOINs or multiple relations
    return get_query_for_node(node.parents[0]).alias(node.parents[0].name)
