"""
Functions for transpiling SQL.

These functions parse the DJ SQL used to define node expressions, and generate SQLAlchemy
queries which can be then executed in specific databases.
"""

# pylint: disable=fixme, unused-argument

from operator import attrgetter
from typing import Optional, Union

from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqloxide import parse_sql

from datajunction.models.node import Node
from datajunction.sql.functions import function_registry
from datajunction.sql.parse import find_nodes_by_key
from datajunction.typing import Expression, Function, Identifier, ParseTree, Value


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

    source = get_source(node, tree)
    projection = get_projection(node, tree, source)
    return projection.select_from(source)


def get_projection(node: Node, tree: ParseTree, source: Select) -> Select:
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

        expressions.append(get_expression(expression, source, alias))

    return select(expressions)


def get_expression(
    expression: Expression,
    source: Select,
    alias: Optional[str] = None,
) -> Union[SqlaFunction, str]:
    """
    Build an expression.
    """
    if "Function" in expression:
        return get_function(expression["Function"], source, alias)
    if "Identifier" in expression:
        return get_identifier(expression["Identifier"], source, alias)
    if "Value" in expression:
        return get_value(expression["Value"], source, alias)
    if expression == "Wildcard":
        return "*"
    raise NotImplementedError(f"Unable to handle expression: {expression}")


def get_function(
    function: Function,
    source: Select,
    alias: Optional[str] = None,
) -> SqlaFunction:
    """
    Build a function.
    """
    name = function["name"][0]["value"]
    args = function["args"]
    evaluated_args = [get_expression(arg["Unnamed"], source) for arg in args]
    func = function_registry[name.upper()]

    return func.get_sqla_function(*evaluated_args).label(alias)


def get_identifier(
    identifier: Identifier,
    source: Select,
    alias: Optional[str] = None,
) -> SqlaColumn:
    """
    Build a column.
    """
    return getattr(source.columns, identifier["value"]).label(alias)


def get_value(
    value: Value,
    source: Select,
    alias: Optional[str] = None,
) -> Union[int, float, text]:
    """
    Build a value.
    """
    if "Number" in value:
        try:
            return int(value["Number"][0])
        except ValueError:
            return float(value["Number"][0])
    elif "SingleQuotedString" in value:
        return text(value["SingleQuotedString"])

    raise NotImplementedError(f"Unable to handle value: {value}")


def get_source(
    node: Node,
    tree: ParseTree,  # pylint: disable=unused-argument
) -> Select:
    """
    Build the ``FROM`` part of a query.
    """
    # For now assume no JOINs or multiple relations
    return get_query_for_node(node.parents[0]).alias(node.parents[0].name)
