"""Error-related scalar types."""
import strawberry

from datajunction_server.errors import ErrorCode as ErrorCode_

ErrorCode = strawberry.enum(ErrorCode_)


@strawberry.type
class DJError:  # pylint: disable=too-few-public-methods
    """
    A DJ error
    """

    code: ErrorCode  # type: ignore
    message: str | None
    context: str | None
