"""
Tests for ``datajunction.queries``.
"""

from pytest_mock import MockerFixture

from datajunction.models import Database, Query
from datajunction.queries import (
    ColumnMetadata,
    Description,
    TypeEnum,
    get_columns_from_description,
    run_query,
)


def test_get_columns_from_description(mocker: MockerFixture) -> None:
    """
    Test ``get_columns_from_description``.
    """
    dialect = mocker.MagicMock()
    dialect.dbapi.STRING = "STRING"
    dialect.dbapi.BINARY = "BINARY"
    dialect.dbapi.NUMBER = "NUMBER"
    dialect.dbapi.DATETIME = "DATETIME"

    description: Description = [
        ("a", "STRING", "", "", "", "", False),
        ("b", "BINARY", "", "", "", "", False),
        ("c", "NUMBER", "", "", "", "", False),
        ("d", "DATETIME", "", "", "", "", False),
        ("e", "INVALID", "", "", "", "", False),
    ]

    assert get_columns_from_description(description, dialect) == [
        ColumnMetadata(name="a", type=TypeEnum.STRING),
        ColumnMetadata(name="b", type=TypeEnum.BINARY),
        ColumnMetadata(name="c", type=TypeEnum.NUMBER),
        ColumnMetadata(name="d", type=TypeEnum.DATETIME),
        ColumnMetadata(name="e", type=TypeEnum.UNKNOWN),
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
    assert columns == [ColumnMetadata(name="1", type=TypeEnum.STRING)]
    assert list(stream) == [(1,)]
