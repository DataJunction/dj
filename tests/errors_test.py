"""
Tests errors.
"""

from datajunction.errors import DJException


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

    exc = DJException(dbapi_exception="ProgrammingError", http_status_code=400)
    assert exc.dbapi_exception == "ProgrammingError"
    assert exc.http_status_code == 400
