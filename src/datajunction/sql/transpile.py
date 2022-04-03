"""
Functions for transpiling SQL.

These functions parse the DJ SQL used to define node expressions, and generate SQLAlchemy
queries which can be then executed in specific databases.
"""

# pylint: disable=unused-argument

import ast
import operator
import re
from typing import Dict, List, Optional, Union

from sqlalchemy import text
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqloxide import parse_sql

from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.models.query import QueryCreate
from datajunction.sql.dag import get_computable_databases
from datajunction.sql.functions import function_registry
from datajunction.sql.parse import find_nodes_by_key
from datajunction.typing import Expression, Function, Identifier, ParseTree, Value

FILTER_RE = re.compile(r"([\w\./_]+)(<=|<|>=|>|!=|=)(.+)")
COMPARISONS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "=": operator.eq,
    "!=": operator.ne,
}


def get_filter(columns: Dict[str, SqlaColumn], filter_: str) -> BinaryExpression:
    """
    Build a SQLAlchemy filter.
    """
    match = FILTER_RE.match(filter_)
    if not match:
        raise Exception(f"Invalid filter: {filter_}")

    name, op, value = match.groups()  # pylint: disable=invalid-name

    if name not in columns:
        raise Exception(f"Invalid column name: {name}")
    column = columns[name]

    if op not in COMPARISONS:
        valid = ", ".join(COMPARISONS)
        raise Exception(f"Invalid operation: {op} (valid: {valid})")
    comparison = COMPARISONS[op]

    try:
        value = ast.literal_eval(value)
    except Exception as ex:
        raise Exception(f"Invalid value: {value}") from ex

    return comparison(column, value)


def get_query_for_node(
    node: Node,
    groupbys: List[str],
    filters: List[str],
) -> QueryCreate:
    """
    Return a DJ QueryCreate object from a given node.
    """
    databases = get_computable_databases(node)
    if not databases:
        raise Exception(f"Unable to compute {node.name} (no common database)")
    database = sorted(databases, key=operator.attrgetter("cost"))[0]

    engine = create_engine(database.URI)
    node_select = get_select_for_node(node, database)

    columns = {
        f"{from_.name}/{column.name}": column
        for from_ in node_select.froms
        for column in from_.columns
    }
    node_select = node_select.filter(
        *[get_filter(columns, filter_) for filter_ in filters]
    ).group_by(*[columns[groupby] for groupby in groupbys])

    sql = str(node_select.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


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

    source = get_source(node, database, tree)
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
    database: Database,
    tree: ParseTree,  # pylint: disable=unused-argument
) -> Select:
    """
    Build the ``FROM`` part of a query.
    """
    # For now assume no JOINs or multiple relations
    return get_select_for_node(node.parents[0], database).alias(node.parents[0].name)
