"""
Type inference based on functions.
"""

# pylint: disable=unused-argument

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, TypeVar, cast

if TYPE_CHECKING:
    from datajunction.models import Column, Node


function_registry: Dict[str, Callable[..., Any]] = {}


class Wildcard:  # pylint: disable=too-few-public-methods
    """
    Represents the star in a SQL expression.
    """


def evaluate_expression(  # pylint: disable=too-many-branches, too-many-return-statements
    parents: List["Node"],
    expression: Any,
    alias: Optional[str] = None,
) -> Any:
    """
    Evaluates a projection expression.
    """
    from datajunction.models import Column  # pylint: disable=import-outside-toplevel

    if "Identifier" in expression:
        name = expression["Identifier"]["value"]
        candidates = []
        for parent in parents:
            for column in parent.columns:
                if column.name == name:
                    candidates.append(column)
                    break

        if len(candidates) != 1:
            raise Exception(f'Unable to determine origin of column "{name}"')

        return candidates[0]

    if "CompoundIdentifier" in expression:
        name = expression["CompoundIdentifier"][-1]["value"]
        parent_name = ".".join(
            part["value"] for part in expression["CompoundIdentifier"][:-1]
        )
        for parent in parents:
            if parent.name == parent_name:
                break
        else:
            raise Exception(
                f'Unable to determine origin of column "{parent_name}.{name}"',
            )

        for column in parent.columns:
            if column.name == name:
                return column

        raise Exception(f'Unable to find column "{name}" in node "{parent.name}"')

    if "Function" in expression:
        name = ".".join(part["value"] for part in expression["Function"]["name"])
        args = expression["Function"]["args"]
        type_ = function_registry[name](parents, args)
        return Column(name=alias, type=type_)

    if "Value" in expression:
        value = expression["Value"]
        if "Number" in value:
            try:
                return int(value["Number"][0])
            except ValueError:
                return float(value["Number"][0])
        if "SingleQuotedString" in value:
            return value["SingleQuotedString"]

        raise NotImplementedError(f"Unable to handle expression: {expression}")

    if expression == "Wildcard":
        return Wildcard

    raise NotImplementedError(f"Unable to evaluate expression: {expression}")


def register(name: str):
    """
    Decorator to register functions and simplify argument passing.
    """

    def decorator(func: FUNCTION_SIGNATURE) -> FUNCTION_SIGNATURE:
        @wraps(func)
        def wrapped(parents: List["Node"], args: List[Any]) -> str:
            evaluated_args = [
                evaluate_expression(parents, arg["Unnamed"], alias=None) for arg in args
            ]
            return func(*evaluated_args)

        function_registry[name] = wrapped

        return cast(FUNCTION_SIGNATURE, wrapped)

    return decorator


FUNCTION_SIGNATURE = TypeVar(  # pylint: disable=invalid-name
    "FUNCTION_SIGNATURE", bound=Callable[..., str],
)


@register("COUNT")
def count(*args: Any) -> str:
    """
    A simple ``COUNT()``.
    """
    return "int"


@register("MAX")
def max_(column: "Column") -> str:
    """
    The ``MAX()`` function.

    Returns the same type as the column.
    """
    return column.type
