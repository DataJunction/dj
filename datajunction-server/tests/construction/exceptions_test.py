"""
Tests for building nodes and extracting dependencies
"""
# pylint: disable=too-many-lines

import pytest

from datajunction_server.construction.exceptions import CompoundBuildException
from datajunction_server.errors import DJError, DJException, ErrorCode


def test_compound_build_exception():
    """
    Test raising a CompoundBuildException
    """
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(False)  # pylint: disable=protected-access
    CompoundBuildException().append(
        error=DJError(
            code=ErrorCode.INVALID_SQL_QUERY,
            message="This SQL is invalid.",
        ),
        message="Testing a compound build exception",
    )

    assert len(CompoundBuildException().errors) == 1
    assert CompoundBuildException().errors[0].code == ErrorCode.INVALID_SQL_QUERY

    assert "Found 1 issue" in str(CompoundBuildException())

    CompoundBuildException().reset()


def test_raise_compound_build_exception():
    """
    Test raising a CompoundBuildException
    """
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(True)  # pylint: disable=protected-access
    with pytest.raises(DJException) as exc_info:
        CompoundBuildException().append(
            error=DJError(
                code=ErrorCode.INVALID_SQL_QUERY,
                message="This SQL is invalid.",
            ),
            message="Testing a compound build exception",
        )

    assert "Testing a compound build exception" in str(exc_info.value)
