"""
SQL functions for type inference.

This file holds all the functions that we want to support in the SQL used to define
nodes. The functions should have a signature compatible with the signature of the SQL
function, and should return the type of the return value.

For example, the ``COUNT()`` function can be used in different ways:

    SELECT COUNT(*) FROM parent;
    SELECT COUNT(1) FROM parent;
    SELECT COUNT(user_id) FROM parent;

Regardless of the argument, it always return an integer. The function definition for it
should then look like this:

    @register('COUNT')
    def cnt(argument: Union[Wildcard, int, Column]) -> str:
        return 'int'

"""

# pylint: disable=unused-argument, missing-function-docstring

from typing import TYPE_CHECKING, Callable, Dict, Union

if TYPE_CHECKING:
    from datajunction.models.database import Column
    from datajunction.sql.lib import Wildcard


def count(argument: Union["Wildcard", "Column", int]) -> str:
    return "int"


def max_(column: "Column") -> str:
    return column.type


function_registry: Dict[str, Callable[..., str]] = {
    "COUNT": count,
    "MAX": max_,
}
