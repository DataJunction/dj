"""
Tests for ``datajunction_server.models.database``.
"""

import datajunction_server.sql.parsing.types as ct
from datajunction_server.models.column import Column
from datajunction_server.models.database import Database
from datajunction_server.models.table import Table


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
