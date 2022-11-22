"""
Functions for type inference.
"""

# pylint: disable=unused-argument

from typing import TYPE_CHECKING, List, Optional, Type, Union, cast

from sqloxide import parse_sql

from dj.models.column import Column
from dj.sql.functions import function_registry
from dj.sql.parse import find_nodes_by_key
from dj.typing import ColumnType, Expression, Function, Identifier, Value

if TYPE_CHECKING:
    from dj.models.node import Node


class Wildcard:  # pylint: disable=too-few-public-methods
    """
    Represents the star in a SQL query.
    """


def infer_columns(query: str, parents: List["Node"]) -> List[Column]:
    """
    Given a a SQL query and parents, infer schema.
    """
    tree = parse_sql(query, dialect="ansi")

    # Use the first projection. We actually want to check that all the projections
    # produce the same columns, and raise an error if not.
    projection = next(find_nodes_by_key(tree, "projection"))

    columns = []
    for expression in projection:
        alias: Optional[str] = None
        if "UnnamedExpr" in expression:
            expression = expression["UnnamedExpr"]
            columns.append(get_column_from_expression(parents, expression, alias))
        elif "ExprWithAlias" in expression:
            alias = expression["ExprWithAlias"]["alias"]["value"]
            expression = expression["ExprWithAlias"]["expr"]
            columns.append(get_column_from_expression(parents, expression, alias))
        elif expression == "Wildcard":
            if len(parents) > 1:
                raise Exception("Wildcard only works for nodes with a single parent")
            columns.extend(parents[0].columns[:])
        else:
            raise NotImplementedError(f"Unable to handle expression: {expression}")

    # name nameless columns
    i = 0
    for column in columns:
        if column.name is None:
            column.name = f"_col{i}"
            i += 1

    return columns


def evaluate_identifier(parents: List["Node"], identifier: Identifier) -> Column:
    """
    Evaluate an "Identifier" node.
    """
    value = identifier["value"]
    candidates = []
    for parent in parents:
        for column in parent.columns:
            if column.name == value:
                candidates.append(column)
                break

    if len(candidates) != 1:
        raise Exception(f'Unable to determine origin of column "{value}"')

    return candidates[0]


def evaluate_compound_identifier(
    parents: List["Node"],
    compound_identifier: List[Identifier],
) -> Column:
    """
    Evaluate a "CompoundIdentifier" node.
    """
    name = compound_identifier[-1]["value"]
    parent_name = ".".join(part["value"] for part in compound_identifier[:-1])
    parent: Optional["Node"] = None
    for parent in parents:
        if parent.name == parent_name:
            break
    else:
        parent = None

    if not parent:
        raise Exception(
            f'Unable to determine origin of column "{parent_name}.{name}"',
        )

    for column in parent.columns:
        if column.name == name:
            return column

    raise Exception(f'Unable to find column "{name}" in node "{parent.name}"')


def evaluate_function(
    parents: List["Node"],
    function: Function,
    alias: Optional[str] = None,
) -> Column:
    """
    Evaluate a "Function" node.
    """
    name = ".".join(part["value"] for part in function["name"])

    args: List[Expression] = []
    for arg in function["args"]:
        if isinstance(arg["Unnamed"], dict) and "Expr" in arg["Unnamed"]:
            args.append(arg["Unnamed"]["Expr"])
        else:
            args.append(cast(Expression, arg["Unnamed"]))
    evaluated_args = [evaluate_expression(parents, arg) for arg in args]
    type_ = function_registry[name].infer_type(*evaluated_args)

    return Column(name=alias, type=type_)


def evaluate_value(
    value: Value,
    alias: Optional[str] = None,
) -> Union[int, float, str]:
    """
    Evaluate a "Value" node.
    """
    if "Number" in value:
        try:
            return int(value["Number"][0])
        except ValueError:
            return float(value["Number"][0])
    elif "SingleQuotedString" in value:
        return value["SingleQuotedString"]

    raise NotImplementedError(f"Unable to handle value: {value}")


def evaluate_expression(
    parents: List["Node"],
    expression: Expression,
    alias: Optional[str] = None,
) -> Union[Column, int, float, str, Type[Wildcard]]:
    """
    Evaluates an expression from a projection.
    """
    if "Identifier" in expression:
        return evaluate_identifier(parents, expression["Identifier"])

    if "CompoundIdentifier" in expression:
        return evaluate_compound_identifier(parents, expression["CompoundIdentifier"])

    if "Function" in expression:
        return evaluate_function(parents, expression["Function"], alias)

    if "Value" in expression:
        return evaluate_value(expression["Value"], alias)

    if expression == "Wildcard":
        return Wildcard

    raise NotImplementedError(f"Unable to evaluate expression: {expression}")


def get_column_from_expression(
    parents: List["Node"],
    expression: Expression,
    alias: Optional[str] = None,
) -> Column:
    """
    Return a column from an expression from a projection.
    """
    value = evaluate_expression(parents, expression, alias)

    if isinstance(value, Column):
        return value

    if isinstance(value, int):
        type_ = ColumnType.INT
    elif isinstance(value, float):
        type_ = ColumnType.FLOAT
    elif isinstance(value, str):
        type_ = ColumnType.STR
    else:
        raise Exception(f"Invalid expression for column: {expression}")

    return Column(name=alias, type=type_)
