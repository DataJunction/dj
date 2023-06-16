"""
Tests for ``dj.models.database``.
"""

import dj.sql.parsing.types as ct
from dj.models.column import Column
from dj.models.database import Database
from dj.models.table import Table


def test_hash() -> None:
    """
    Test the hash method to compare models.
    """
    database = Database(id=1, name="test", URI="sqlite://")
    assert database in {database}

    table = Table(id=1, database=database, table="table")
    assert table in {table}

    column = Column(id=1, name="test", type=ct.IntegerType(), table=table)
    assert column in {column}
