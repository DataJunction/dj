"""
Functions for transpiling SQL.

These functions parse the DJ SQL used to define node expressions, and generate SQLAlchemy
queries which can be then executed in specific databases.
"""

# pylint: disable=unused-argument

import operator
from typing import Any, List, Optional, Union, cast

from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqloxide import parse_sql

from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.sql.functions import function_registry
from datajunction.sql.parse import find_nodes_by_key
from datajunction.typing import (
    BinaryOp,
    Expression,
    Function,
    Identifier,
    ParseTree,
    Value,
)

OPERATIONS = {
    "Gt": operator.gt,
    "GtEq": operator.ge,
    "Lt": operator.lt,
    "LtEq": operator.le,
    "Eq": operator.eq,
    "NotEq": operator.ne,
}


def get_select_for_node(node: Node, database: Database) -> Select:
    """
    Build a SQLAlchemy ``select()`` for a given node.
    """
    # if the node is materialized we use the table with the cheapest cost
    tables = [table for table in node.tables if table.database == database]
    if tables:
        table = sorted(tables, key=operator.attrgetter("cost"))[0]
        engine = create_engine(table.database.URI)
        materialized_table = Table(
            table.table,
            MetaData(bind=engine),
            schema=table.schema_,
            autoload=True,
        )
        return select(materialized_table)

    tree = parse_sql(node.expression, dialect="ansi")
    return get_query(tree, node.parents, database)


def get_query(tree: ParseTree, parents: List[Node], database: Database) -> Select:
    """
    Build a SQLAlchemy query.
    """
    # SELECT ... FROM ...
    source = get_source(parents, database, tree)
    projection = get_projection(tree, source)
    query = projection.select_from(source)

    # WHERE ...
    selection = get_selection(tree, source)
    if selection is not None:
        query = query.filter(selection)

    # GROUP BY ...
    groupby = get_groupby(tree, source)
    if groupby:
        query = query.group_by(*groupby)

    # LIMIT ...
    limit = get_limit(tree, source)
    if limit:
        query = query.limit(limit)

    # TODO (betodealmeida): HAVING, ORDER BY, etc.

    return query


def get_limit(tree: ParseTree, source: Select) -> Optional[int]:
    """
    Return the ``LIMIT`` of a query.
    """
    limit = next(find_nodes_by_key(tree, "limit"))
    if limit is None:
        return None

    return cast(int, get_expression(limit, source))


def get_groupby(tree: ParseTree, source: Select) -> List[Any]:
    """
    Build the ``GROUP BY`` clause of a query.
    """
    groupby = next(find_nodes_by_key(tree, "group_by"))
    return [get_expression(expression, source) for expression in groupby]


def get_selection(
    tree: ParseTree,
    source: Select,
) -> Union[SqlaFunction, SqlaColumn, ClauseElement, int, float, str, text, None]:
    """
    Build the ``WHERE`` clause of a query.
    """
    selection = next(find_nodes_by_key(tree, "selection"))
    if not selection:
        return None

    return get_expression(selection, source)


def get_binary_op(selection: BinaryOp, source: Select) -> ClauseElement:
    """
    Build a binary operation (eg, >).
    """
    left = get_expression(selection["left"], source)
    right = get_expression(selection["right"], source)
    op = selection["op"]  # pylint: disable=invalid-name

    if op not in OPERATIONS:
        raise NotImplementedError(f"Operator not supported: {op}")

    return OPERATIONS[op](left, right)


def get_projection(tree: ParseTree, source: Select) -> Select:
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

        expression = get_expression(expression, source)
        if hasattr(expression, "label"):
            expression = expression.label(alias)
        expressions.append(expression)

    return select(expressions)


def get_expression(
    expression: Expression,
    source: Select,
) -> Union[SqlaFunction, SqlaColumn, ClauseElement, int, float, str, text]:
    """
    Build an expression.
    """
    if "Function" in expression:
        return get_function(expression["Function"], source)
    if "Identifier" in expression:
        return get_identifier(expression["Identifier"], source)
    if "Value" in expression:
        return get_value(expression["Value"], source)
    if "BinaryOp" in expression:
        return get_binary_op(expression["BinaryOp"], source)
    if expression == "Wildcard":
        return "*"
    raise NotImplementedError(f"Unable to handle expression: {expression}")


def get_function(
    function: Function,
    source: Select,
) -> SqlaFunction:
    """
    Build a function.
    """
    name = function["name"][0]["value"]
    args = function["args"]
    evaluated_args = [get_expression(arg["Unnamed"], source) for arg in args]
    func = function_registry[name.upper()]

    # TODO (betodealmeida): pass dialect
    return func.get_sqla_function(*evaluated_args)


def get_identifier(
    identifier: Identifier,
    source: Select,
) -> SqlaColumn:
    """
    Build a column.
    """
    return getattr(source.columns, identifier["value"])


def get_value(
    value: Value,
    source: Select,
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
    parents: List[Node],
    database: Database,
    tree: ParseTree,  # pylint: disable=unused-argument
) -> Select:
    """
    Build the ``FROM`` part of a query.
    """
    # For now assume no JOINs or multiple relations
    return get_select_for_node(parents[0], database).alias(parents[0].name)
