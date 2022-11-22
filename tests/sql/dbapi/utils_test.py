"""
Tests for ``dj.sql.dbapi.utils``.
"""

from dj.sql.dbapi.utils import escape_parameter


def test_escape_parameter() -> None:
    """
    Test ``escape_parameter``.
    """
    assert escape_parameter("*") == "*"
    assert escape_parameter("foo") == "'foo'"
    assert escape_parameter("O'Malley's") == "'O''Malley''s'"
    assert escape_parameter(True) == "TRUE"
    assert escape_parameter(False) == "FALSE"
    assert escape_parameter(1) == "1"
    assert escape_parameter(1.0) == "1.0"
