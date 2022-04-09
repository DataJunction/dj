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
                "id": 6,
                "name": "core.num_comments",
                "description": "Number of comments",
                "created_at": "2022-04-08T14:24:20.281542",
                "updated_at": "2022-04-08T16:03:13.224116",
                "expression": "SELECT COUNT(*) FROM core.comments",
                "dimensions": [
                    "core.comments.id",
                    "core.comments.user_id",
                    "core.comments.timestamp",
                    "core.comments.text",
                ],
            },
        ],
    )
    requests_mock.get(
        "http://localhost:8000/nodes/",
        json=[
            {
                "id": 4,
                "name": "core.comments",
                "description": "A fact table with comments",
                "created_at": "2022-04-08T14:24:17.912559",
                "updated_at": "2022-04-08T16:03:12.839833",
                "expression": None,
                "is_metric": False,
                "columns": [
                    {"id": None, "name": "id", "type": "INT", "table_id": None},
                    {"id": None, "name": "user_id", "type": "INT", "table_id": None},
                    {
                        "id": None,
                        "name": "timestamp",
                        "type": "DATETIME",
                        "table_id": None,
                    },
                    {"id": None, "name": "text", "type": "STR", "table_id": None},
                ],
            },
            {
                "id": 5,
                "name": "core.users",
                "description": "A user dimension table",
                "created_at": "2022-04-08T14:24:20.269593",
                "updated_at": "2022-04-08T16:03:13.208912",
                "expression": None,
                "is_metric": False,
                "columns": [
                    {"id": None, "name": "id", "type": "INT", "table_id": None},
                    {"id": None, "name": "full_name", "type": "STR", "table_id": None},
                    {"id": None, "name": "age", "type": "INT", "table_id": None},
                    {"id": None, "name": "country", "type": "STR", "table_id": None},
                    {"id": None, "name": "gender", "type": "STR", "table_id": None},
                    {
                        "id": None,
                        "name": "preferred_language",
                        "type": "STR",
                        "table_id": None,
                    },
                    {
                        "id": None,
                        "name": "secret_number",
                        "type": "FLOAT",
                        "table_id": None,
                    },
                ],
            },
            {
                "id": 6,
                "name": "core.num_comments",
                "description": "Number of comments",
                "created_at": "2022-04-08T14:24:20.281542",
                "updated_at": "2022-04-08T16:03:13.224116",
                "expression": "SELECT COUNT(*) FROM core.comments",
                "is_metric": True,
                "columns": [
                    {"id": None, "name": "_col0", "type": "INT", "table_id": None},
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
    ]
