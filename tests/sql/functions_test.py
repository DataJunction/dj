"""
Tests for ``datajunction.sql.functions``.
"""

from sqlalchemy import String
from sqlalchemy.sql.schema import Column as SqlaColumn

from datajunction.models.database import Column
from datajunction.sql.functions import Count, Max
from datajunction.typing import ColumnType


def test_count() -> None:
    """
    Test ``Count``.
    """
    assert Count.infer_type("*") == ColumnType.INT
    assert str(Count.get_sqla_function("*")) == "count(:count_1)"


def test_max() -> None:
    """
    Test ``Max``.
    """
    column = Column(name="ds", type=ColumnType.STR)
    sqla_column = SqlaColumn("ds", String)

    assert Max.infer_type(column) == ColumnType.STR
    assert str(Max.get_sqla_function(sqla_column)) == "max(ds)"
