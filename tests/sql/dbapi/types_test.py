"""
Tests for ``datajunction.sql.dbapi.types``.
"""

from pytest_mock import MockerFixture
from yarl import URL

from datajunction.sql.dbapi import connect
from datajunction.sql.dbapi.types import (
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from datajunction.typing import ColumnType


def test_types(mocker: MockerFixture) -> None:
    """
    Test that native Python types can be used in queries.
    """
    requests = mocker.patch("datajunction.sql.dbapi.cursor.requests")
    url = URL("http://localhost:8000/")
    headers = {"Content-Type": "application/json"}

    connection = connect(url)
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE test_types (
            type_date DATE,
            type_time TIME,
            type_timestamp TIMESTAMP,
            type_binary BLOB
        )
    """,
    )
    cursor.execute(
        (
            "SELECT * FROM some_table "
            "WHERE type_date=%(type_date)s "
            "AND type_time=%(type_time)s "
            "AND type_timestamp=%(type_timestamp)s "
            "AND type_binary=%(type_binary)s"
        ),
        {
            "type_date": Date(2020, 1, 1),
            "type_time": Time(0, 0, 0),
            "type_timestamp": Timestamp(2020, 1, 1, 0, 0, 0),
            "type_binary": Binary("🦥"),
        },
    )
    requests.post.assert_called_with(
        url / "queries/",
        json={
            "database_id": 0,
            "submitted_query": (
                "SELECT * FROM some_table "
                "WHERE type_date='2020-01-01' "
                "AND type_time='00:00:00+00:00' "
                "AND type_timestamp='2020-01-01 00:00:00+00:00' "
                "AND type_binary='🦥'"
            ),
        },
        headers=headers,
    )

    cursor.execute(
        (
            "SELECT * FROM some_table "
            "WHERE type_date=%(type_date)s "
            "AND type_time=%(type_time)s "
            "AND type_timestamp=%(type_timestamp)s "
            "AND type_binary=%(type_binary)s"
        ),
        {
            "type_date": DateFromTicks(1),
            "type_time": TimeFromTicks(2),
            "type_timestamp": TimestampFromTicks(3),
            "type_binary": Binary("🦥"),
        },
    )
    requests.post.assert_called_with(
        url / "queries/",
        json={
            "database_id": 0,
            "submitted_query": (
                "SELECT * FROM some_table "
                "WHERE type_date='1970-01-01' "
                "AND type_time='00:00:02+00:00' "
                "AND type_timestamp='1970-01-01 00:00:03+00:00' "
                "AND type_binary='🦥'"
            ),
        },
        headers=headers,
    )


def test_comparison() -> None:
    """
    Test type comparison.
    """
    assert STRING == ColumnType.STR
    assert (STRING == 1) is False
