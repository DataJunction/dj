"""
Tests for the SQLAlchemy dialect.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from sqlalchemy.engine.url import make_url
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

    dbapi_connection.cursor.side_effect = Exception("BOOM!")
    assert not dialect.do_ping(dbapi_connection)


def test_get_schema_names(mocker: MockerFixture) -> None:
    """
    Test ``get_schema_names``.
    """
    connection = mocker.MagicMock()
    dialect = DJDialect()

    assert dialect.get_schema_names(connection) == ["main"]


def test_superset_methods(mocker: MockerFixture) -> None:
    """
    Test the methods that are needed for Apache Superset integration.
    """
    connection = mocker.MagicMock()
    dialect = DJDialect()

    assert dialect.get_pk_constraint(connection, "metrics") == {
        "constrained_columns": [],
        "name": None,
    }
    assert not dialect.get_foreign_keys(connection, "metrics")
    assert not dialect.get_check_constraints(connection, "metrics")
    assert not dialect.get_indexes(connection, "metrics")
    assert not dialect.get_unique_constraints(connection, "metrics")
    assert dialect.get_table_comment(connection, "metrics") == {"text": ""}


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
    connection.engine.connect().connection.base_url = URL("http://localhost:8000/")
    requests_mock.get(
        "http://localhost:8000/metrics/",
        json=[
            {
                "id": 3,
                "name": "core.num_comments",
                "description": "Number of comments",
                "created_at": "2022-04-10T20:23:01.961078",
                "updated_at": "2022-04-10T20:23:01.961083",
                "query": "SELECT COUNT(*) FROM core.comments",
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
                "query": None,
                "columns": [
                    {
                        "name": "id",
                        "type": "INT",
                    },
                    {
                        "name": "user_id",
                        "type": "INT",
                    },
                    {
                        "name": "timestamp",
                        "type": "DATETIME",
                    },
                    {
                        "name": "text",
                        "type": "STR",
                    },
                    {
                        "name": "__time",
                        "type": "DATETIME",
                    },
                    {
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
                "query": None,
                "columns": [
                    {
                        "name": "id",
                        "type": "INT",
                    },
                    {
                        "name": "full_name",
                        "type": "STR",
                    },
                    {
                        "name": "age",
                        "type": "INT",
                    },
                    {
                        "name": "country",
                        "type": "STR",
                    },
                    {
                        "name": "gender",
                        "type": "STR",
                    },
                    {
                        "name": "preferred_language",
                        "type": "STR",
                    },
                    {
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
                "query": "SELECT COUNT(*) FROM core.comments",
                "columns": [
                    {
                        "name": "_col0",
                        "type": "INT",
                    },
                ],
            },
        ],
    )
    dialect = DJDialect()

    assert not dialect.get_columns(connection, "not-metrics")
    assert [
        column["name"] for column in dialect.get_columns(connection, "metrics")
    ] == [
        "core.comments.id",
        "core.comments.user_id",
        "core.comments.timestamp",
        "core.comments.text",
        "core.comments.__time",
        "core.comments.count",
    ]
    assert [
        str(column["type"]) for column in dialect.get_columns(connection, "metrics")
    ] == [
        "INTEGER",
        "INTEGER",
        "DATETIME",
        "TEXT",
        "DATETIME",
        "INTEGER",
    ]
