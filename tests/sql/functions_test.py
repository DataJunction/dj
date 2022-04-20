"""
Tests for ``datajunction.sql.functions``.
"""

import pytest
from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn

from datajunction.errors import DJInvalidInputException
from datajunction.models.column import Column
from datajunction.sql.functions import Coalesce, Count, Max
from datajunction.typing import ColumnType

from .utils import query_to_string


def test_count() -> None:
    """
    Test ``Count``.
    """
    assert Count.infer_type("*") == ColumnType.INT
    assert query_to_string(Count.get_sqla_function("*")) == "count('*')"


def test_max() -> None:
    """
    Test ``Max``.
    """
    column = Column(name="ds", type=ColumnType.STR)
    sqla_column = SqlaColumn("ds", String)

    assert Max.infer_type(column) == ColumnType.STR
    assert query_to_string(Max.get_sqla_function(sqla_column)) == "max(ds)"


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
    assert str(excinfo.value) == "All arguments MUST have the same type"
    assert excinfo.value.errors[0].message == (
        "All arguments passed to `COALESCE` MUST have the same type. If the columns "
        "have different types they need to be cast to a common type."
    )
    assert excinfo.value.errors[0].debug == {"types": [ColumnType.INT, ColumnType.STR]}

    with pytest.raises(DJInvalidInputException) as excinfo:
        Coalesce.infer_type()
    assert str(excinfo.value) == "Wrong number of arguments to function"
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
