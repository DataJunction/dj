"""
Errors and warnings.
"""

import inspect
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Type, TypedDict

from sqlmodel import SQLModel


class ErrorCode(int, Enum):
    """
    Error codes.
    """

    # generic errors
    UNKNOWN_ERROR = 0
    NOT_IMPLEMENTED_ERROR = 1
    ALREADY_EXISTS = 2

    # metric API
    INVALID_FILTER_PATTERN = 100
    INVALID_COLUMN_IN_FILTER = 101
    INVALID_VALUE_IN_FILTER = 102

    # SQL API
    INVALID_ARGUMENTS_TO_FUNCTION = 200
    INVALID_SQL_QUERY = 201
    MISSING_COLUMNS = 202
    UNKNOWN_NODE = 203
    NODE_TYPE_ERROR = 204
    INVALID_DIMENSION_JOIN = 205
    INVALID_COLUMN = 206

    # SQL Build Error
    COMPOUND_BUILD_EXCEPTION = 300
    MISSING_PARENT = 201


class DebugType(TypedDict, total=False):
    """
    Type for debug information.
    """

    # link to where an issue can be filed
    issue: str

    # link to documentation about the problem
    documentation: str

    # any additional context
    context: Dict[str, Any]


class DJErrorType(TypedDict):
    """
    Type for serialized errors.
    """

    code: int
    message: str
    debug: Optional[DebugType]


class DJError(SQLModel):
    """
    An error.
    """

    code: ErrorCode
    message: str
    debug: Optional[Dict[str, Any]]
    context: str = ""

    def __str__(self) -> str:
        """
        Format the error nicely.
        """
        context = f" from `{self.context}`" if self.context else ""
        return f"{self.message}{context} (error code: {self.code})"


class DJWarningType(TypedDict):
    """
    Type for serialized warnings.
    """

    code: Optional[int]
    message: str
    debug: Optional[DebugType]


class DJWarning(SQLModel):
    """
    A warning.
    """

    code: Optional[ErrorCode] = None
    message: str
    debug: Optional[Dict[str, Any]]


DBAPIExceptions = Literal[
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]


class DJExceptionType(TypedDict):
    """
    Type for serialized exceptions.
    """

    title: str
    status: int
    detail: Optional[str]
    errors: List[DJErrorType]
    warnings: List[DJWarningType]


class DJException(Exception):
    """
    Base class for errors.
    """

    # The "detail" member, if present, ought to focus on helping the client
    # correct the problem, rather than giving debugging information.
    detail: str = "Please contact your administrator."

    # exception that should be raised when ``DJException`` is caught by the DB API cursor
    dbapi_exception: DBAPIExceptions = "Error"

    # status code that should be returned when ``DJException`` is caught by the API layer
    http_status_code: int = 500

    def __init__(  # pylint: disable=too-many-arguments
        self,
        title: str,
        http_status_code: Optional[int] = None,
        errors: Optional[List[DJError]] = None,
        warnings: Optional[List[DJWarning]] = None,
        dbapi_exception: Optional[DBAPIExceptions] = None,
    ):
        self.title = title
        self.errors = errors or []
        self.warnings = warnings or []

        if dbapi_exception is not None:
            self.dbapi_exception = dbapi_exception
        if http_status_code is not None:
            self.http_status_code = http_status_code

        super().__init__(self.title)

    def to_dict(self) -> DJExceptionType:
        """
        Convert to dict.
        """
        return {
            "title": self.title,
            "status": self.http_status_code,
            "detail": self.detail,
            "errors": [error.dict() for error in self.errors],
            "warnings": [warning.dict() for warning in self.warnings],
        }

    def __str__(self) -> str:
        """
        Format the exception nicely.
        """
        if not self.errors:
            return self.title

        plural = "s" if len(self.errors) > 1 else ""
        combined_errors = "\n".join(f"- {error}" for error in self.errors)
        errors = f"The following error{plural} happened:\n{combined_errors}"

        return f"{self.title}\n{errors}"

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, DJException)
            and self.title == other.title
            and self.detail == other.detail
            and self.errors == other.errors
            and self.warnings == other.warnings
            and self.dbapi_exception == other.dbapi_exception
            and self.http_status_code == other.http_status_code
        )


class DJInvalidInputException(DJException):
    """
    Exception raised when the input provided by the user is invalid.
    """

    detail = (
        "The user provided input is invalid. Please try modifying your request "
        "and sending it again. Retrying the same request will likely result in "
        "the same error."
    )
    dbapi_exception: DBAPIExceptions = "ProgrammingError"
    http_status_code: int = 422


class DJNotImplementedException(DJException):
    """
    Exception raised when some functionality hasn't been implemented in DJ yet.
    """

    detail = (
        "This functionality hasn't been implemented in DJ yet. Please file a "
        "ticket at https://github.com/DataJunction/dj/issues describing the "
        "problem and intended use case."
    )
    dbapi_exception: DBAPIExceptions = "NotSupportedError"
    http_status_code: int = 500


class DJInternalErrorException(DJException):
    """
    Exception raised when we do something wrong in the code.
    """

    detail = (
        "An unexpected internal error occurred. Please contact your "
        "adminstrator if the error persists."
    )
    dbapi_exception: DBAPIExceptions = "InternalError"
    http_status_code: int = 500


class ExceptionRegistry:  # pylint: disable=too-few-public-methods
    """
    A singleton to register exceptions.
    """

    def __init__(self):
        self.exceptions = {
            name: class_
            for name, class_ in globals().items()
            if inspect.isclass(class_) and issubclass(class_, DJException)
        }

    def register(self, exception: Type[DJException]) -> None:
        """
        Register an exception.
        """
        self.exceptions[exception.__name__] = exception

    def get(self, name: str) -> Optional[DJException]:
        """
        Return an exception given its name.
        """
        return self.exceptions.get(name)


registry = ExceptionRegistry()
