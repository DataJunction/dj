"""
Errors and warnings.
"""
from http import HTTPStatus
from typing import Any, Dict, List, Literal, Optional, TypedDict

from pydantic.main import BaseModel

from datajunction_server.enum import IntEnum


class ErrorCode(IntEnum):
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
    QUERY_SERVICE_ERROR = 207
    INVALID_ORDER_BY = 208

    # SQL Build Error
    COMPOUND_BUILD_EXCEPTION = 300
    MISSING_PARENT = 301
    TYPE_INFERENCE = 302

    # Authentication
    AUTHENTICATION_ERROR = 400
    OAUTH_ERROR = 401
    INVALID_LOGIN_CREDENTIALS = 402
    USER_NOT_FOUND = 403

    # Authorization
    UNAUTHORIZED_ACCESS = 500
    INCOMPLETE_AUTHORIZATION = 501

    # Node validation
    INVALID_PARENT = 600
    INVALID_DIMENSION = 601
    INVALID_METRIC = 602


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


class DJError(BaseModel):
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


class DJErrorException(Exception):
    """
    Wrapper allows raising DJError
    """

    def __init__(self, dj_error: DJError):
        self.dj_error = dj_error


class DJWarningType(TypedDict):
    """
    Type for serialized warnings.
    """

    code: Optional[int]
    message: str
    debug: Optional[DebugType]


class DJWarning(BaseModel):
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
        return (
            isinstance(other, DJException)
            and self.message == other.message
            and self.errors == other.errors
            and self.warnings == other.warnings
            and self.dbapi_exception == other.dbapi_exception
            and self.http_status_code == other.http_status_code
        )


class DJNodeNotFound(DJException):
    """
    Exception raised when a given node name is not found.
    """

    http_status_code: int = HTTPStatus.NOT_FOUND


class DJInvalidInputException(DJException):
    """
    Exception raised when the input provided by the user is invalid.
    """

    dbapi_exception: DBAPIExceptions = "ProgrammingError"
    http_status_code: int = HTTPStatus.UNPROCESSABLE_ENTITY


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


class DJAlreadyExistsException(DJException):
    """
    Exception raised when trying to create an entity that already exists.
    """

    dbapi_exception: DBAPIExceptions = "DataError"
    http_status_code: int = HTTPStatus.CONFLICT


class DJDoesNotExistException(DJException):
    """
    Exception raised when an entity doesn't exist.
    """

    dbapi_exception: DBAPIExceptions = "DataError"
    http_status_code: int = HTTPStatus.NOT_FOUND


class DJQueryServiceClientException(DJException):
    """
    Exception raised when the query service returns an error
    """

    dbapi_exception: DBAPIExceptions = "InterfaceError"
    http_status_code: int = 500


class DJActionNotAllowedException(DJException):
    """
    Exception raised when an action is not allowed.
    """


class DJPluginNotFoundException(DJException):
    """
    Exception raised when plugin is not found.
    """


class DJQueryBuildException(DJException):
    """
    Exception raised when query building fails.
    """


class DJQueryBuildError(DJError):
    """
    Query build error
    """
