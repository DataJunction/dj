"""
Type inference.
"""

# pylint: disable=unused-argument

from typing import TYPE_CHECKING, Any, List, Optional, Type, Union

from datajunction.models.database import Column
from datajunction.sql.functions import function_registry

if TYPE_CHECKING:
    from datajunction.models.node import Node


class Wildcard:  # pylint: disable=too-few-public-methods
    """
    Represents the star in a SQL expression.
    """


def evaluate_identifier(parents: List["Node"], expression: Any) -> "Column":
    """
    Evaluate an "Identifier" node.
    """
    value = expression["value"]
    candidates = []
    for parent in parents:
        for column in parent.columns:
            if column.name == value:
                candidates.append(column)
                break

    if len(candidates) != 1:
        raise Exception(f'Unable to determine origin of column "{value}"')

    return candidates[0]


def evaluate_compound_identifier(parents: List["Node"], expression: Any) -> "Column":
    """
    Evaluate a "CompoundIdentifier" node.
    """
    name = expression[-1]["value"]
    parent_name = ".".join(part["value"] for part in expression[:-1])
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
    expression: Any,
    alias: Optional[str] = None,
) -> "Column":
    """
    Evaluate a "Function" node.
    """
    name = ".".join(part["value"] for part in expression["name"])
    args = expression["args"]
    evaluated_args = [evaluate_expression(parents, arg["Unnamed"]) for arg in args]
    type_ = function_registry[name](*evaluated_args)

    return Column(name=alias, type=type_)


def evaluate_value(
    expression: Any,
    alias: Optional[str] = None,
) -> Union[int, float, str]:
    """
    Evaluate a "Value" node.
    """
    if "Number" in expression:
        try:
            return int(expression["Number"][0])
        except ValueError:
            return float(expression["Number"][0])
    elif "SingleQuotedString" in expression:
        return expression["SingleQuotedString"]

    raise NotImplementedError(f"Unable to handle expression: {expression}")


def evaluate_expression(
    parents: List["Node"],
    expression: Any,
    alias: Optional[str] = None,
) -> Union["Column", int, float, str, Type[Wildcard]]:
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
    expression: Any,
    alias: Optional[str] = None,
) -> "Column":
    """
    Return a column from an expression from a projection.
    """
    value = evaluate_expression(parents, expression, alias)

    if isinstance(value, Column):
        return value

    if isinstance(value, int):
        type_ = "int"
    elif isinstance(value, float):
        type_ = "float"
    elif isinstance(value, str):
        type_ = "str"
    else:
        raise Exception(f"Invalid expression for column: {expression}")

    return Column(name=alias, type=type_)
