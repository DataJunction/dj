"""
Functions for transpiling SQL.

These functions parse the DJ SQL used to define node expressions, and generate SQLAlchemy
queries which can be then executed in specific databases.
"""

# pylint: disable=unused-argument

import operator
from typing import Any, Dict, List, Optional, Set, Union, cast

from sqlalchemy import case, desc, text
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import Select, select
from sqlalchemy.sql.expression import ClauseElement, TextClause, literal
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqloxide import parse_sql

from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.sql.dag import get_referenced_columns_from_sql
from datajunction.sql.functions import function_registry
from datajunction.sql.parse import find_nodes_by_key
from datajunction.typing import (
    BinaryOp,
    Case,
    Expression,
    Function,
    Identifier,
    JoinOperator,
    ParseTree,
    Relation,
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

# this probably needs more types
SqlaExpression = Union[
    SqlaFunction,
    SqlaColumn,
    ClauseElement,
    int,
    float,
    str,
    text,
    bool,
]


def get_select_for_node(
    node: Node,
    database: Database,
    columns: Optional[Set[str]] = None,
) -> Select:
    """
    Build a SQLAlchemy ``select()`` for a given node.
    """
    # if no columns are specified, require all
    if columns is None:
        columns = {column.name for column in node.columns}

    engine = create_engine(database.URI, **database.extra_params)

    # if the node is materialized we use the table with the cheapest cost, as long as it
    # has all the requested columns (see #104)
    tables = [
        table
        for table in node.tables
        if table.database == database
        and columns <= {column.name for column in table.columns}
    ]
    if tables:
        table = sorted(tables, key=operator.attrgetter("cost"))[0]
        materialized_table = Table(
            table.table,
            MetaData(bind=engine),
            schema=table.schema_,
            autoload=True,
        )
        return select(materialized_table)

    tree = parse_sql(node.expression, dialect="ansi")
    return get_query(node.expression, node.parents, tree, database, engine.dialect.name)


def get_query(
    expression: Optional[str],
    parents: List[Node],
    tree: ParseTree,
    database: Database,
    dialect: Optional[str] = None,
) -> Select:
    """
    Build a SQLAlchemy query.
    """
    # SELECT ... FROM ...
    if parents:
        source = get_source(expression, parents, database, tree, dialect)
        projection = get_projection(tree, source, dialect)
        query = projection.select_from(source)
    else:
        source = None
        query = get_projection(tree, source, dialect)

    # WHERE ...
    selection = get_selection(tree, source, dialect)
    if selection is not None:
        query = query.filter(selection)

    # GROUP BY ...
    groupby = get_groupby(tree, source, dialect)
    if groupby:
        query = query.group_by(*groupby)

    # HAVING ...
    having = get_having(tree, source, dialect)
    if having is not None:
        query = query.having(having)

    # ORDER BY ...
    orderby = get_orderby(tree, source, dialect)
    if orderby:
        query = query.order_by(*orderby)

    # LIMIT ...
    limit = get_limit(tree, source, dialect)
    if limit:
        query = query.limit(limit)

    return query


def get_limit(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> Optional[int]:
    """
    Return the ``LIMIT`` of a query.
    """
    limit = next(find_nodes_by_key(tree, "limit"))
    if limit is None:
        return None

    return cast(int, get_expression(limit, source, dialect))


def get_orderby(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> List[Any]:
    """
    Build the ``ORDER BY`` clause of a query.
    """
    orderbys = next(find_nodes_by_key(tree, "order_by"))

    expressions = []
    for orderby in orderbys:
        expression = get_expression(orderby["expr"], source, dialect)
        if orderby.get("asc") is False:
            expression = desc(expression)
        expressions.append(expression)

    return expressions


def get_groupby(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> List[Any]:
    """
    Build the ``GROUP BY`` clause of a query.
    """
    groupby = next(find_nodes_by_key(tree, "group_by"))
    return [get_expression(expression, source, dialect) for expression in groupby]


def get_having(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> Optional[ClauseElement]:
    """
    Build the ``HAVING`` clause of a query.
    """
    having = next(find_nodes_by_key(tree, "having"))
    if having is None:
        return None

    return get_binary_op(having["BinaryOp"], source, source, dialect)


def get_selection(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaExpression:
    """
    Build the ``WHERE`` clause of a query.
    """
    selection = next(find_nodes_by_key(tree, "selection"))
    if not selection:
        return None

    return get_expression(selection, source, dialect)


def get_binary_op(
    selection: BinaryOp,
    left_source: Optional[Select] = None,
    right_source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> ClauseElement:
    """
    Build a binary operation (eg, >).
    """
    left = get_expression(selection["left"], left_source, dialect)
    right = get_expression(selection["right"], right_source, dialect)
    op = selection["op"]  # pylint: disable=invalid-name

    if op not in OPERATIONS:
        raise NotImplementedError(f"Operator not supported: {op}")

    return OPERATIONS[op](left, right)


def get_projection(
    tree: ParseTree,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> Select:
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

        expression = get_expression(expression, source, dialect)
        if alias:
            expression = add_alias(expression, alias)
        expressions.append(expression)

    return select(expressions)


def add_alias(expression: SqlaExpression, alias: str) -> SqlaExpression:
    """
    Add an alias to an expression as a label.
    """
    if hasattr(expression, "label"):
        return expression.label(alias)  # type: ignore
    if isinstance(expression, TextClause):
        return literal(str(expression)).label(alias)
    return literal(expression).label(alias)


def get_expression(  # pylint: disable=too-many-return-statements
    expression: Expression,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaExpression:
    """
    Build an expression.
    """
    if "Function" in expression:
        return get_function(expression["Function"], source, dialect)
    if "Identifier" in expression:
        return get_identifier(expression["Identifier"], source, dialect)
    if "CompoundIdentifier" in expression:
        return get_compound_identifier(
            expression["CompoundIdentifier"],
            source,
            dialect,
        )
    if "Value" in expression:
        return get_value(expression["Value"], source, dialect)
    if "BinaryOp" in expression:
        return get_binary_op(expression["BinaryOp"], source, source, dialect)
    if "Case" in expression:
        return get_case(expression["Case"], source, dialect)
    if expression == "Wildcard":
        return "*"
    raise NotImplementedError(f"Unable to handle expression: {expression}")


def get_case(
    case_: Case,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaFunction:
    """
    Build a ``CASE`` statement.
    """
    conditions = [
        get_expression(condition, source, dialect) for condition in case_["conditions"]
    ]
    results = [get_expression(result, source, dialect) for result in case_["results"]]
    value = (
        get_expression(case_["operand"], source, dialect) if case_["operand"] else None
    )
    else_ = (
        get_expression(case_["else_result"], source, dialect)
        if case_["else_result"]
        else None
    )
    return case(*zip(conditions, results), else_=else_, value=value)


def get_function(
    function: Function,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaFunction:
    """
    Build a function.
    """
    name = function["name"][0]["value"]

    args: List[Expression] = []
    for arg in function["args"]:
        if isinstance(arg["Unnamed"], dict) and "Expr" in arg["Unnamed"]:
            args.append(arg["Unnamed"]["Expr"])
        else:
            args.append(cast(Expression, arg["Unnamed"]))
    evaluated_args = [get_expression(arg, source, dialect) for arg in args]
    func = function_registry[name]

    return func.get_sqla_function(*evaluated_args, dialect=dialect)


def get_identifier(
    identifier: Identifier,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaColumn:
    """
    Build a column.
    """
    if source is None:
        raise Exception("Unable to return identifier without a source")
    return getattr(source.columns, identifier["value"])


def get_compound_identifier(
    compound_identifier: List[Identifier],
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> SqlaColumn:
    """
    Build a column.

    This assumes the first part of the identifier is ``source``.
    """
    if source is None:
        raise Exception("Unable to return identifier without a source")

    table_name = compound_identifier[0]["value"]
    column_name = compound_identifier[1]["value"]
    for column in source.columns:
        if column.table.name == table_name and column.name == column_name:
            return column

    raise Exception("No column found")  # pragma: no cover


def get_value(
    value: Value,
    source: Optional[Select] = None,
    dialect: Optional[str] = None,
) -> Union[int, float, text, bool]:
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
    elif "Boolean" in value:
        return value["Boolean"]

    raise NotImplementedError(f"Unable to handle value: {value}")


def get_node_from_relation(relation: Relation, parent_map: Dict[str, Node]) -> Node:
    """
    Return a node from a relation.
    """
    name = ".".join(part["value"] for part in relation["Table"]["name"])
    return parent_map[name]


def get_source(  # pylint: disable=too-many-locals
    expression: Optional[str],
    parents: List[Node],
    database: Database,
    tree: ParseTree,
    dialect: Optional[str] = None,
) -> Select:
    """
    Build the ``FROM`` part of a query.
    """
    parent_columns = get_referenced_columns_from_sql(expression, parents)
    parent_map = {parent.name: parent for parent in parents}

    # Parse the ``FROM`` clause. We only support single statements, so ``tree`` should
    # have a single element. We also don't support ``CROSS JOIN``, so there should be
    # only 1 element in the ``from`` key.
    from_ = tree[0]["Query"]["body"]["Select"]["from"][0]

    # find the first node
    from_node = get_node_from_relation(from_["relation"], parent_map)
    source = get_select_for_node(
        from_node,
        database,
        parent_columns[from_node.name],
    ).alias(from_node.name)

    # apply joins, if any
    previous_subquery = source
    for join in from_["joins"]:
        join_node = get_node_from_relation(join["relation"], parent_map)
        subquery = get_select_for_node(
            join_node,
            database,
            parent_columns[join_node.name],
        ).alias(join_node.name)

        join_operator = cast(JoinOperator, join["join_operator"])
        condition = get_binary_op(
            join_operator["Inner"]["On"]["BinaryOp"],
            previous_subquery,
            subquery,
            dialect,
        )
        source = source.join(subquery, condition)
        previous_subquery = subquery

    return source
