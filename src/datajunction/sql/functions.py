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
        argument: Union["Wildcard", Column, int], dialect: Optional[str] = None
    ) -> SqlaFunction:
        return func.count(argument)

The ``dialect`` can be used to build custom functions.

"""

# pylint: disable=unused-argument, missing-function-docstring, arguments-differ, too-many-return-statements

import abc
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from sqlalchemy.sql import cast, func
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.sql.functions import Function as SqlaFunction
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.sql.sqltypes import TIMESTAMP, DateTime

from datajunction.errors import (
    DJError,
    DJInternalErrorException,
    DJInvalidInputException,
    DJNotImplementedException,
    ErrorCode,
)
from datajunction.models.column import Column
from datajunction.typing import ColumnType
from datajunction.utils import get_issue_url

if TYPE_CHECKING:
    from datajunction.sql.lib import Wildcard


# a subset of the SQLAlchemy dialects that support ``DATE_TRUNC``
DATE_TRUNC_DIALECTS = {
    "postgresql",
    "trino",
    "presto",
}

# family of SQLite dialects
SQLITE_DIALECTS = {
    "sqlite",
    "shillelagh",
    "gsheets",
}

# Druid uses ISO durations for ``DATE_TRUNC``
ISO_DURATIONS = {
    "second": "PT1S",
    "minute": "PT1M",
    "hour": "PT1H",
    "day": "P1D",
    "week": "P1W",
    "month": "P1M",
    "quarter": "P3M",
    "year": "P1Y",
}


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
    def get_sqla_function(*, dialect: Optional[str] = None) -> SqlaFunction:
        raise NotImplementedError("Subclass MUST implement get_sqla_function")


class Count(Function):
    """
    The ``COUNT`` function.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(argument: Union["Wildcard", Column, int]) -> ColumnType:  # type: ignore
        return ColumnType.INT

    @staticmethod
    def get_sqla_function(  # type: ignore
        argument: Union[SqlaColumn, str, int],
        *,
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        return func.count(argument)


class DateTruncNotImplementException(DJNotImplementedException):
    """
    Custom exception for resolutions not implemented in ``DATE_TRUNC``.
    """

    def __init__(self, dialect: str, resolution: str):
        issue_url = str(
            get_issue_url(title=f"Resolution missing for {dialect}: {resolution}"),
        )
        docs_url = (
            "https://github.com/DataJunction/datajunction/blob/main/docs/functions.rst"
            "#date-trunc"
        )
        super().__init__(
            message=f'Resolution "{resolution}" not supported by dialect "{dialect}"',
            errors=[
                DJError(
                    code=ErrorCode.NOT_IMPLEMENTED_ERROR,
                    message=(
                        f'The resolution "{resolution}" in the `DATE_TRUNC` function '
                        f'hasn\'t been implemented in DJ for the dialect "{dialect}" '
                        f"yet. You can file an issue at {issue_url} to request it to "
                        f"be added, or use the documentation at {docs_url} to implement "
                        "it."
                    ),
                    debug={
                        "issue": issue_url,
                        "documentation": docs_url,
                        "context": {
                            "dialect": dialect,
                            "function": "DATE_TRUNC",
                            "resolution": resolution,
                        },
                    },
                ),
            ],
        )


class DateTrunc(Function):
    """
    The ``DATE_TRUNC`` function.

    There's no standard ``DATE_TRUNC`` function, so we implement it for every dialect that
    doesn't support it.
    """

    is_aggregation = False

    @staticmethod
    def infer_type(resolution: str, column: Column) -> ColumnType:  # type: ignore
        return ColumnType.DATETIME

    # pylint: disable=too-many-branches
    @staticmethod
    def get_sqla_function(  # type: ignore
        resolution: TextClause,
        column: SqlaColumn,
        *,
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        if dialect is None:
            raise DJInternalErrorException(
                message="A dialect is needed for `DATE_TRUNC`",
            )

        if dialect in DATE_TRUNC_DIALECTS:
            return func.date_trunc(str(resolution), column, type_=DateTime)

        if dialect in SQLITE_DIALECTS:
            if str(resolution) == "second":
                return func.datetime(
                    func.strftime("%Y-%m-%dT%H:%M:%S", column),
                    type_=DateTime,
                )
            if str(resolution) == "minute":
                return func.datetime(
                    func.strftime("%Y-%m-%dT%H:%M:00", column),
                    type_=DateTime,
                )
            if str(resolution) == "hour":
                return func.datetime(
                    func.strftime("%Y-%m-%dT%H:00:00", column),
                    type_=DateTime,
                )
            if str(resolution) == "day":
                return func.datetime(column, "start of day", type_=DateTime)
            if str(resolution) == "week":
                # https://stackoverflow.com/a/51666243
                return func.datetime(
                    column,
                    "1 day",
                    "weekday 0",
                    "-7 days",
                    "start of day",
                    type_=DateTime,
                )
            if str(resolution) == "month":
                return func.datetime(column, "start of month", type_=DateTime)
            if str(resolution) == "quarter":
                return func.datetime(
                    column,
                    func.printf("-%d month", (func.strftime("%m", column) - 1) % 3 + 1),
                    type_=DateTime,
                )
            if str(resolution) == "year":
                return func.datetime(column, "start of year", type_=DateTime)

            raise DateTruncNotImplementException(dialect, resolution)

        if dialect == "druid":
            if str(resolution) not in ISO_DURATIONS:
                raise DateTruncNotImplementException(dialect, resolution)

            return func.time_floor(
                cast(column, TIMESTAMP),
                ISO_DURATIONS[str(resolution)],
                type_=DateTime,
            )

        issue_url = get_issue_url(title=f"DATE_TRUNC for {dialect}")
        docs_url = (
            "https://github.com/DataJunction/datajunction/blob/main/docs/functions.rst"
            "#date-trunc"
        )
        raise DJNotImplementedException(
            message=f'Dialect "{dialect}" doesn\'t support `DATE_TRUNC`',
            errors=[
                DJError(
                    code=ErrorCode.NOT_IMPLEMENTED_ERROR,
                    message=(
                        f'The function "DATE_TRUNC" hasn\'t been implemented for '
                        f'dialect "{dialect}" in DJ yet. You can file an issue at '
                        f"{issue_url} to request it to be added, or use the "
                        f"documentation at {docs_url} to implement it."
                    ),
                    debug={
                        "issue": issue_url,
                        "documentation": docs_url,
                        "context": {"dialect": dialect},
                    },
                ),
            ],
        )


class Max(Function):
    """
    The ``MAX`` function.
    """

    is_aggregation = True

    @staticmethod
    def infer_type(column: Column) -> ColumnType:  # type: ignore
        return column.type

    @staticmethod
    def get_sqla_function(  # type: ignore
        column: SqlaColumn,
        *,
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        return func.max(column)


class Coalesce(Function):
    """
    The ``COALESCE`` function.
    """

    is_aggregation = False

    @staticmethod
    def infer_type(*args: Any) -> ColumnType:
        """
        Coalesce requires that all arguments have the same type.
        """
        types: List[ColumnType] = [
            arg.type
            if isinstance(arg, Column)
            else ColumnType(type(arg).__name__.upper())
            for arg in args
        ]

        if not types:
            raise DJInvalidInputException(
                message="Wrong number of arguments to function",
                errors=[
                    DJError(
                        code=ErrorCode.INVALID_ARGUMENTS_TO_FUNCTION,
                        message="You need to pass at least one argument to `COALESCE`.",
                    ),
                ],
            )

        if len(set(types)) > 1:
            raise DJInvalidInputException(
                message="All arguments MUST have the same type",
                errors=[
                    DJError(
                        code=ErrorCode.INVALID_ARGUMENTS_TO_FUNCTION,
                        message=(
                            "All arguments passed to `COALESCE` MUST have the same "
                            "type. If the columns have different types they need to be "
                            "cast to a common type."
                        ),
                        debug={"context": {"types": types}},
                    ),
                ],
            )

        return types.pop()

    @staticmethod
    def get_sqla_function(  # type: ignore
        *args: Any,
        dialect: Optional[str] = None,
    ) -> SqlaFunction:
        return func.coalesce(*args)


class FunctionRegistry:  # pylint: disable=too-few-public-methods
    """
    A simple object for registering functions.
    """

    def __init__(self, functions: Dict[str, Type[Function]]):
        self.functions = functions

    def __getitem__(self, name: str) -> Type[Function]:
        name = name.upper()

        if name in self.functions:
            return self.functions[name]

        issue_url = str(get_issue_url(title=f"Function missing: {name}"))
        docs_url = (
            "https://github.com/DataJunction/datajunction/blob/main/docs/functions.rst"
        )

        raise DJNotImplementedException(
            message=f"The function `{name}` hasn't been implemented yet",
            errors=[
                DJError(
                    code=ErrorCode.NOT_IMPLEMENTED_ERROR,
                    message=(
                        f"The function `{name}` hasn't been implemented in DJ yet. "
                        f"You can file an issue at {issue_url} to request it to be "
                        f"added, or use the documentation at {docs_url} to implement it."
                    ),
                    debug={
                        "issue": issue_url,
                        "documentation": docs_url,
                        "context": {"function": name},
                    },
                ),
            ],
        )


function_registry = FunctionRegistry(
    {
        "COALESCE": Coalesce,
        "COUNT": Count,
        "DATE_TRUNC": DateTrunc,
        "MAX": Max,
    },
)
