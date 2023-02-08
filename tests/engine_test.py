"""
Tests for ``dj.engine``.
"""
# pylint: disable=invalid-name

from pytest_mock import MockerFixture

from dj.engine import (
    ColumnMetadata,
    Description,
    get_columns_from_description,
    run_query,
)
from dj.models.database import Database
from dj.models.query import Query
from dj.typing import ColumnType


def test_get_columns_from_description(mocker: MockerFixture) -> None:
    """
    Test ``get_columns_from_description``.
    """
    dialect = mocker.MagicMock()
    dialect.dbapi.STRING = "STRING"
    dialect.dbapi.BINARY = "BINARY"
    dialect.dbapi.NUMBER = "NUMBER"
    dialect.dbapi.TIMESTAMP = "TIMESTAMP"

    description: Description = [
        ("a", "STRING", "", "", "", "", False),
        ("b", "BINARY", "", "", "", "", False),
        ("c", "NUMBER", "", "", "", "", False),
        ("d", "TIMESTAMP", "", "", "", "", False),
        ("e", "INVALID", "", "", "", "", False),
    ]

    assert get_columns_from_description(description, dialect) == [
        ColumnMetadata(name="a", type=ColumnType.STR),
        ColumnMetadata(name="b", type=ColumnType.BYTES),
        ColumnMetadata(name="c", type=ColumnType.FLOAT),
        ColumnMetadata(name="d", type=ColumnType.TIMESTAMP),
        ColumnMetadata(name="e", type=ColumnType.STR),
    ]


def test_run_query() -> None:
    """
    Test ``run_query``.
    """
    database = Database(name="test", URI="sqlite://")
    query = Query(
        database=database,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
    )
    sql, columns, stream = run_query(query)[0]
    assert sql == "SELECT 1"
    assert columns == [ColumnMetadata(name="1", type=ColumnType.STR)]
    assert list(stream) == [(1,)]
