"""
Tests for ``datajunction.sql.functions``.
"""
# pylint: disable=line-too-long

import pytest
from sqlalchemy import Integer, String
from sqlalchemy.sql.schema import Column as SqlaColumn

from datajunction.errors import DJInvalidInputException, DJNotImplementedException
from datajunction.models.column import Column
from datajunction.sql.functions import (
    Avg,
    Coalesce,
    Count,
    Max,
    Min,
    Now,
    Sum,
    function_registry,
)
from datajunction.typing import ColumnType

from .utils import query_to_string


def test_count() -> None:
    """
    Test ``Count`` function.
    """
    assert Count.infer_type("*") == ColumnType.INT
    assert query_to_string(Count.get_sqla_function("*")) == "count('*')"


def test_min() -> None:
    """
    Test ``Min`` function.
    """
    column = Column(name="dateint", type=ColumnType.INT)
    sqla_column = SqlaColumn("dateint", Integer)

    assert Min.infer_type(column) == ColumnType.INT
    assert query_to_string(Min.get_sqla_function(sqla_column)) == "min(dateint)"


def test_max() -> None:
    """
    Test ``Max`` function.
    """
    column = Column(name="ds", type=ColumnType.STR)
    sqla_column = SqlaColumn("ds", String)

    assert Max.infer_type(column) == ColumnType.STR
    assert query_to_string(Max.get_sqla_function(sqla_column)) == "max(ds)"


def test_now() -> None:
    """
    Test ``Now`` function.
    """
    assert Now.infer_type() == ColumnType.DATETIME
    assert query_to_string(Now.get_sqla_function()) == "now()"


def test_coalesce_infer_type() -> None:
    """
    Test type inference in the ``Coalesce`` function.
    """
    assert (
        Coalesce.infer_type(
            Column(name="user_id", type=ColumnType.INT),
            Column(name="event_id", type=ColumnType.INT),
            1,
        )
        == ColumnType.INT
    )

    with pytest.raises(DJInvalidInputException) as excinfo:
        Coalesce.infer_type(
            Column(name="user_id", type=ColumnType.INT),
            Column(name="name", type=ColumnType.STR),
        )
    assert (
        str(excinfo.value)
        == """All arguments MUST have the same type
The following error happened:
- All arguments passed to `COALESCE` MUST have the same type. If the columns have different types they need to be cast to a common type. (error code: 200)"""
    )
    assert excinfo.value.errors[0].message == (
        "All arguments passed to `COALESCE` MUST have the same type. If the columns "
        "have different types they need to be cast to a common type."
    )
    assert excinfo.value.errors[0].debug == {
        "context": {"types": [ColumnType.INT, ColumnType.STR]},
    }

    with pytest.raises(DJInvalidInputException) as excinfo:
        Coalesce.infer_type()
    assert (
        str(excinfo.value)
        == """Wrong number of arguments to function
The following error happened:
- You need to pass at least one argument to `COALESCE`. (error code: 200)"""
    )
    assert excinfo.value.errors[0].message == (
        "You need to pass at least one argument to `COALESCE`."
    )


def test_coalesce_get_sqla_function() -> None:
    """
    Test the transpilation of the ``Coalesce`` function to SQLAlchemy.
    """
    sqla_column = SqlaColumn("ds", String)
    assert (
        query_to_string(Coalesce.get_sqla_function(sqla_column, "test"))
        == "coalesce(ds, 'test')"
    )


def test_missing_functions() -> None:
    """
    Test missing functions.
    """
    with pytest.raises(DJNotImplementedException) as excinfo:
        function_registry["INVALID_FUNCTION"]  # pylint: disable=pointless-statement
    assert (
        str(excinfo.value)
        == """The function `INVALID_FUNCTION` hasn't been implemented yet
The following error happened:
- The function `INVALID_FUNCTION` hasn't been implemented in DJ yet. You can file an issue at https://github.com/DataJunction/datajunction/issues/new?title=Function+missing:+INVALID_FUNCTION to request it to be added, or use the documentation at https://github.com/DataJunction/datajunction/blob/main/docs/functions.rst to implement it. (error code: 1)"""
    )


def test_sum() -> None:
    """
    Test ``sum`` function.
    """
    assert Sum.infer_type(Column(name="value", type=ColumnType.INT)) == ColumnType.INT
    assert (
        Sum.infer_type(Column(name="value", type=ColumnType.FLOAT)) == ColumnType.FLOAT
    )
    assert (
        query_to_string(Sum.get_sqla_function(SqlaColumn("value", Integer)))
        == "sum(value)"
    )


def test_avg() -> None:
    """
    Test ``avg`` function.
    """
    assert Avg.infer_type(Column(name="value", type=ColumnType.INT)) == ColumnType.FLOAT
    assert (
        query_to_string(Avg.get_sqla_function(SqlaColumn("value", Integer)))
        == "avg(value)"
    )
