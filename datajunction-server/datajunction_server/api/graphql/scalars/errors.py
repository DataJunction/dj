"""Error-related scalar types."""

import strawberry

from datajunction_server.errors import ErrorCode as ErrorCode_

ErrorCode = strawberry.enum(ErrorCode_)


@strawberry.type
class DJError:
    """
    A DJ error
    """

    code: ErrorCode  # type: ignore
    message: str | None
    context: str | None
