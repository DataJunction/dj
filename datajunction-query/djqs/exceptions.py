"""
Errors and warnings.
"""

from typing import Any, Dict, List, Literal, Optional, TypedDict

from sqlmodel import SQLModel

from djqs.enum import IntEnum


class ErrorCode(IntEnum):
    """
    Error codes.
    """

    # generic errors
    UNKWNON_ERROR = 0
    NOT_IMPLEMENTED_ERROR = 1
    ALREADY_EXISTS = 2

    # metric API
    INVALID_FILTER_PATTERN = 100
    INVALID_COLUMN_IN_FILTER = 101
    INVALID_VALUE_IN_FILTER = 102

    # SQL API
    INVALID_ARGUMENTS_TO_FUNCTION = 200


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

    def __str__(self) -> str:
        """
        Format the error nicely.
        """
        return f"{self.message} (error code: {self.code})"


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

    message: Optional[str]
    errors: List[DJErrorType]
    warnings: List[DJWarningType]


class DJException(Exception):
    """
    Base class for errors.
    """

    message: str
    errors: List[DJError]
    warnings: List[DJWarning]

    # exception that should be raised when ``DJException`` is caught by the DB API cursor
    dbapi_exception: DBAPIExceptions = "Error"

    # status code that should be returned when ``DJException`` is caught by the API layer
    http_status_code: int = 500

    def __init__(  # pylint: disable=too-many-arguments
        self,
        message: Optional[str] = None,
        errors: Optional[List[DJError]] = None,
        warnings: Optional[List[DJWarning]] = None,
        dbapi_exception: Optional[DBAPIExceptions] = None,
        http_status_code: Optional[int] = None,
    ):
        self.errors = errors or []
        self.warnings = warnings or []
        self.message = message or "\n".join(error.message for error in self.errors)

        if dbapi_exception is not None:
            self.dbapi_exception = dbapi_exception
        if http_status_code is not None:
            self.http_status_code = http_status_code

        super().__init__(self.message)

    def to_dict(self) -> DJExceptionType:
        """
        Convert to dict.
        """
        return {  # pragma: no cover
            "message": self.message,
            "errors": [error.dict() for error in self.errors],
            "warnings": [warning.dict() for warning in self.warnings],
        }

    def __str__(self) -> str:
        """
        Format the exception nicely.
        """
        if not self.errors:
            return self.message

        plural = "s" if len(self.errors) > 1 else ""
        combined_errors = "\n".join(f"- {error}" for error in self.errors)
        errors = f"The following error{plural} happened:\n{combined_errors}"

        return f"{self.message}\n{errors}"

    def __eq__(self, other) -> bool:
        return (  # pragma: no cover
            isinstance(other, DJException)
            and self.message == other.message
            and self.errors == other.errors
            and self.warnings == other.warnings
            and self.dbapi_exception == other.dbapi_exception
            and self.http_status_code == other.http_status_code
        )


class DJInvalidInputException(DJException):
    """
    Exception raised when the input provided by the user is invalid.
    """

    dbapi_exception: DBAPIExceptions = "ProgrammingError"
    http_status_code: int = 422


class DJNotImplementedException(DJException):
    """
    Exception raised when some functionality hasn't been implemented in DJ yet.
    """

    dbapi_exception: DBAPIExceptions = "NotSupportedError"
    http_status_code: int = 500


class DJInternalErrorException(DJException):
    """
    Exception raised when we do something wrong in the code.
    """

    dbapi_exception: DBAPIExceptions = "InternalError"
    http_status_code: int = 500


class DJInvalidTableRef(DJException):
    """
    Raised for invalid table values
    """


class DJTableNotFound(DJException):
    """
    Raised for tables that cannot be found
    """
