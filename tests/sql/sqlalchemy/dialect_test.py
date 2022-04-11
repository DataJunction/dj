"""
Tests for the SQLAlchemy dialect.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import sqltypes
from yarl import URL

from datajunction.sql import dbapi
from datajunction.sql.sqlalchemy.dialect import DJDialect


def test_dbapi() -> None:
    """
    Test the ``dbapi`` classmethod.
    """
    assert DJDialect.dbapi() is dbapi


def test_create_connect_args() -> None:
    """
    Test ``create_connect_args``.
    """
    dialect = DJDialect()

    assert dialect.create_connect_args(make_url("dj://localhost:8000/0")) == (
        (URL("http://localhost:8000/"), 0),
        {},
    )
    assert dialect.create_connect_args(
        make_url("dj://localhost:8000/0?scheme=https"),
    ) == (
        (URL("https://localhost:8000/"), 0),
        {},
    )
    assert dialect.create_connect_args(make_url("dj://localhost:8000/mount/0")) == (
        (URL("http://localhost:8000/mount"), 0),
        {},
    )


def test_do_ping(mocker: MockerFixture) -> None:
    """
    Test ``do_ping``.
    """
    dbapi_connection = mocker.MagicMock()
    dialect = DJDialect()

    assert dialect.do_ping(dbapi_connection)
    dbapi_connection.cursor().execute.assert_called_with("SELECT 1")


def test_has_table(mocker: MockerFixture) -> None:
    """
    Test ``has_table``.
    """
    connection = mocker.MagicMock()
    dialect = DJDialect()

    assert dialect.has_table(connection, "metrics")
    assert not dialect.has_table(connection, "core.comments")


def test_get_table_names(mocker: MockerFixture) -> None:
    """
    Test ``get_table_names``.
    """
    connection = mocker.MagicMock()
    dialect = DJDialect()

    assert dialect.get_table_names(connection) == ["metrics"]


def test_get_columns(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test ``get_columns``.
    """
    connection = mocker.MagicMock()
    connection.base_url = URL("http://localhost:8000/")
    requests_mock.get(
        "http://localhost:8000/metrics/",
        json=[
            {
                "id": 3,
                "name": "core.num_comments",
                "description": "Number of comments",
                "created_at": "2022-04-10T20:23:01.961078",
                "updated_at": "2022-04-10T20:23:01.961083",
                "expression": "SELECT COUNT(*) FROM core.comments",
                "dimensions": [
                    "core.comments.id",
                    "core.comments.user_id",
                    "core.comments.timestamp",
                    "core.comments.text",
                    "core.comments.__time",
                    "core.comments.count",
                ],
            },
        ],
    )
    requests_mock.get(
        "http://localhost:8000/nodes/",
        json=[
            {
                "id": 1,
                "name": "core.comments",
                "description": "A fact table with comments",
                "created_at": "2022-04-10T20:22:58.345198",
                "updated_at": "2022-04-10T20:22:58.345201",
                "type": "source",
                "expression": None,
                "columns": [
                    {
                        "id": 14,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "id",
                        "type": "INT",
                    },
                    {
                        "id": 15,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "user_id",
                        "type": "INT",
                    },
                    {
                        "id": 16,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "timestamp",
                        "type": "DATETIME",
                    },
                    {
                        "id": 17,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "text",
                        "type": "STR",
                    },
                    {
                        "id": 18,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "__time",
                        "type": "DATETIME",
                    },
                    {
                        "id": 19,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "count",
                        "type": "INT",
                    },
                ],
            },
            {
                "id": 2,
                "name": "core.users",
                "description": "A user dimension table",
                "created_at": "2022-04-10T20:23:01.333020",
                "updated_at": "2022-04-10T20:23:01.333024",
                "type": "dimension",
                "expression": None,
                "columns": [
                    {
                        "id": 33,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "id",
                        "type": "INT",
                    },
                    {
                        "id": 34,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "full_name",
                        "type": "STR",
                    },
                    {
                        "id": 35,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "age",
                        "type": "INT",
                    },
                    {
                        "id": 36,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "country",
                        "type": "STR",
                    },
                    {
                        "id": 37,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "gender",
                        "type": "STR",
                    },
                    {
                        "id": 38,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "preferred_language",
                        "type": "STR",
                    },
                    {
                        "id": 39,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "secret_number",
                        "type": "FLOAT",
                    },
                ],
            },
            {
                "id": 3,
                "name": "core.num_comments",
                "description": "Number of comments",
                "created_at": "2022-04-10T20:23:01.961078",
                "updated_at": "2022-04-10T20:23:01.961083",
                "type": "metric",
                "expression": "SELECT COUNT(*) FROM core.comments",
                "columns": [
                    {
                        "id": 40,
                        "dimension_id": None,
                        "dimension_column": None,
                        "name": "_col0",
                        "type": "INT",
                    },
                ],
            },
        ],
    )
    dialect = DJDialect()

    assert not dialect.get_columns(connection, "not-metrics")
    assert dialect.get_columns(connection, "metrics") == [
        {
            "name": "core.comments.id",
            "type": sqltypes.INTEGER,
            "nullable": True,
            "default": None,
        },
        {
            "name": "core.comments.user_id",
            "type": sqltypes.INTEGER,
            "nullable": True,
            "default": None,
        },
        {
            "name": "core.comments.timestamp",
            "type": sqltypes.DATETIME,
            "nullable": True,
            "default": None,
        },
        {
            "name": "core.comments.text",
            "type": sqltypes.TEXT,
            "nullable": True,
            "default": None,
        },
        {
            "name": "core.comments.__time",
            "type": sqltypes.DATETIME,
            "nullable": True,
            "default": None,
        },
        {
            "name": "core.comments.count",
            "type": sqltypes.INTEGER,
            "nullable": True,
            "default": None,
        },
    ]
