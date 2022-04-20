"""
Errors and warnings.
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, TypedDict

from sqlmodel import SQLModel


class ErrorCode(int, Enum):
    """
    Error codes.
    """

    UNKWNON_ERROR = 0

    # metric API
    INVALID_FILTER_PATTERN = 100
    INVALID_COLUMN_IN_FILTER = 101
    INVALID_VALUE_IN_FILTER = 102

    # SQL API
    INVALID_ARGUMENTS_TO_FUNCTION = 200


class DJErrorType(TypedDict):
    """
    Type for serialized errors.
    """

    code: int
    message: str
    debug: Optional[Dict[str, Any]]


class DJError(SQLModel):
    """
    An error.
    """

    code: ErrorCode
    message: str
    debug: Optional[Dict[str, Any]]


class DJWarningType(TypedDict):
    """
    Type for serialized warnings.
    """

    code: Optional[int]
    message: str
    debug: Optional[Dict[str, Any]]


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
        return {
            "message": self.message,
            "errors": [error.dict() for error in self.errors],
            "warnings": [warning.dict() for warning in self.warnings],
        }


class DJInvalidInputException(DJException):

    """
    Exception raised when the input provided by the user is invalid.
    """

    dbapi_exception: DBAPIExceptions = "ProgrammingError"
    http_status_code: int = 422
