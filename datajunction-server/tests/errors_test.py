"""
Tests errors.
"""

from http import HTTPStatus

from datajunction_server.errors import DJError, DJException, ErrorCode


def test_dj_exception() -> None:
    """
    Test the base ``DJException``.
    """
    exc = DJException()
    assert exc.dbapi_exception == "Error"
    assert exc.http_status_code == 500

    exc = DJException(dbapi_exception="InternalError")
    assert exc.dbapi_exception == "InternalError"
    assert exc.http_status_code == 500

    exc = DJException(
        dbapi_exception="ProgrammingError",
        http_status_code=HTTPStatus.BAD_REQUEST,
    )
    assert exc.dbapi_exception == "ProgrammingError"
    assert exc.http_status_code == HTTPStatus.BAD_REQUEST

    exc = DJException("Message")
    assert str(exc) == "Message"
    exc = DJException(
        "Message",
        errors=[
            DJError(message="Error 1", code=ErrorCode.UNKNOWN_ERROR),
            DJError(message="Error 2", code=ErrorCode.UNKNOWN_ERROR),
        ],
    )
    assert (
        str(exc)
        == """Message
The following errors happened:
- Error 1 (error code: 0)
- Error 2 (error code: 0)"""
    )

    assert DJException("Message") == DJException("Message")
