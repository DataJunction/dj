"""
Tests for ``datajunction.models.database``.
"""

from datajunction.models.database import Column, Database, Table
from datajunction.typing import ColumnType


def test_hash() -> None:
    """
    Test the hash method to compare models.
    """
    database = Database(id=1, name="test", URI="sqlite://")
    assert database in {database}
    assert database.to_yaml() == {
        "description": "",
        "URI": "sqlite://",
        "read-only": True,
        "async_": False,
        "cost": 1.0,
    }

    table = Table(id=1, database=database, table="table")
    assert table in {table}
    assert table.to_yaml() == {
        "catalog": None,
        "schema": None,
        "table": "table",
        "cost": 1.0,
    }

    column = Column(id=1, name="test", type=ColumnType.INT, table=table)
    assert column in {column}
    assert column.to_yaml() == {"type": "INT"}
