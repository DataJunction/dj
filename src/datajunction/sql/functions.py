"""
SQL functions for type inference.

This file holds all the functions that we want to support in the SQL used to define
nodes. The functions are used to infer types and transpile the (DJ) SQL to SQLAlchemy.

For type inference each class representing a function should have a method with a
signature compatible with the signature of the SQL function, and should return the type of
the return value.

For example, the ``COUNT()`` function can be used in different ways:

    SELECT COUNT(*) FROM parent;
    SELECT COUNT(1) FROM parent;
    SELECT COUNT(user_id) FROM parent;

Regardless of the argument, it always return an integer. The method definition for it
should then look like this:

    @staticmethod
    def infer_type(argument: Union[Wildcard, int, Column]) -> ColumnType:
        return ColumnType.INT

For tranpilation:

    @staticmethod
    def get_sqla_function(
        argument: Union["Wildcard", "Column", int], dialect: Optional[str] = None
    ) -> SqlaFunction:
        return func.count(argument)

The ``dialect`` can be used to build custom functions.

"""

# pylint: disable=unused-argument, missing-function-docstring, arguments-differ, fixme

import abc
from typing import TYPE_CHECKING, Any, Dict, Optional, Type, Union

from sqlalchemy.sql import func
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqlalchemy.sql.schema import Column as SqlaColumn

from datajunction.typing import ColumnType

if TYPE_CHECKING:
    from datajunction.models.database import Column
    from datajunction.sql.lib import Wildcard


class Function:  # pylint: disable=too-few-public-methods
    """
    A DJ function.
    """

    is_aggregation = False

    @staticmethod
    @abc.abstractmethod
    def infer_type(*args: Any) -> ColumnType:
        raise NotImplementedError("Subclass MUST implement infer_type")

    @staticmethod
    @abc.abstractmethod
    def get_sqla_function(dialect: Optional[str] = None) -> SqlaFunction:
        raise NotImplementedError("Subclass MUST implement get_sqla_function")


class Count(Function):
    """
    The ``COUNT`` function.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(argument: Union["Wildcard", "Column", int]) -> ColumnType:  # type: ignore
        return ColumnType.INT

    @staticmethod
    def get_sqla_function(  # type: ignore
        argument: Union[SqlaColumn, str, int],
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        return func.count(argument)


class Max(Function):
    """
    The ``MAX`` function.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(column: "Column") -> ColumnType:  # type: ignore
        return column.type

    @staticmethod
    def get_sqla_function(  # type: ignore
        column: SqlaColumn,
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        return func.max(column)


function_registry: Dict[str, Type[Function]] = {
    "COUNT": Count,
    "MAX": Max,
}
