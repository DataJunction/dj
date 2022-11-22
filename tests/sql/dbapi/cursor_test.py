"""
Tests for ``dj.sql.dbapi.cursor``.
"""
# pylint: disable=redefined-builtin

from http import HTTPStatus

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from dj.sql.dbapi.cursor import Cursor
from dj.sql.dbapi.exceptions import (
    InternalError,
    NotSupportedError,
    ProgrammingError,
    Warning,
)


def test_cursor_execute(mocker: MockerFixture) -> None:
    """
    Test the ``execute`` method.
    """
    requests = mocker.patch("dj.sql.dbapi.cursor.requests")
    requests.post().headers.get.return_value = "application/json"
    url = URL("http://localhost:8000/")
    headers = {
        "Content-Type": "application/msgpack",
        "Accept": "application/msgpack; q=1.0, application/json; q=0.5",
    }
    cursor = Cursor(url)

    cursor.execute("SELECT 1")
    requests.post.assert_called_with(
        url / "queries/",
        data=b"\x82\xabdatabase_id\x00\xafsubmitted_query\xa8SELECT 1",
        headers=headers,
        timeout=60,
    )

    cursor.execute("SELECT * FROM some_table WHERE name = %(name)s", {"name": "Alice"})
    requests.post.assert_called_with(
        url / "queries/",
        data=(
            b"\x82\xabdatabase_id\x00\xafsubmitted_query\xd9-SELECT * FROM some_t"
            b"able WHERE name = 'Alice'"
        ),
        headers=headers,
        timeout=60,
    )


def test_cursor_execute_msgpack(mocker: MockerFixture) -> None:
    """
    Test the ``execute`` method with msgpack.
    """
    msgpack = mocker.patch("dj.sql.dbapi.cursor.msgpack")
    msgpack.packb.return_value = b"data"
    requests = mocker.patch("dj.sql.dbapi.cursor.requests")
    requests.post().headers.get.return_value = "application/msgpack"
    url = URL("http://localhost:8000/")
    headers = {
        "Content-Type": "application/msgpack",
        "Accept": "application/msgpack; q=1.0, application/json; q=0.5",
    }
    cursor = Cursor(url)

    cursor.execute("SELECT 1")
    requests.post.assert_called_with(
        url / "queries/",
        data=b"data",
        headers=headers,
        timeout=60,
    )
    msgpack.unpackb.assert_called()


def test_cursor_execute_invalid_content_type(mocker: MockerFixture) -> None:
    """
    Test the ``execute`` method with an invalid content type.
    """
    requests = mocker.patch("dj.sql.dbapi.cursor.requests")
    requests.post().headers.get.return_value = "application/protobuf"
    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    with pytest.raises(Exception) as excinfo:
        cursor.execute("SELECT 1")
    assert str(excinfo.value) == "Unable to parse content type: application/protobuf"


def test_cursor_execute_error(requests_mock: Mocker) -> None:
    """
    Test the ``execute`` method when an error is returned.
    """
    requests_mock.post(
        "http://localhost:8000/queries/",
        json={
            "message": "The query is invalid",
            "errors": [
                {
                    "code": 0,
                    "message": "An unknown error occurred",
                    "debug": {"query": "SELECT A FROM metrics"},
                },
            ],
            "warnings": [
                {
                    "code": None,
                    "message": "Your quote is low",
                    "debug": {"current": 4, "limit": 5},
                },
            ],
        },
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        headers={
            "X-DJ-Error": "true",
            "X-DBAPI-Exception": "ProgrammingError",
            "Content-Type": "application/json",
        },
    )

    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.execute("SELECT 1")
    assert str(excinfo.value) == "The query is invalid"

    requests_mock.post(
        "http://localhost:8000/queries/",
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
    )

    with pytest.raises(InternalError) as excinfo:
        cursor.execute("SELECT 1")
    assert (
        str(excinfo.value) == "It is pitch black. You are likely to be eaten by a grue."
    )


def test_cursor_execute_multiple_statements() -> None:
    """
    Test that the ``execute`` method raises a warning on multiple statements.
    """
    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    with pytest.raises(Warning) as excinfo:
        cursor.execute("SELECT 1; SELECT 2")
    assert str(excinfo.value) == "You can only execute one statement at a time"


def test_execute_many() -> None:
    """
    Test ``execute_many``.
    """
    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    with pytest.raises(NotSupportedError) as excinfo:
        cursor.executemany(
            "SELECT * FROM some_table WHERE name = %(name)s",
            [{"name": "Alice"}, {"name": "Bob"}],
        )
    assert (
        str(excinfo.value)
        == "``executemany`` is not supported, use ``execute`` instead"
    )


def test_fetch_methods(mocker: MockerFixture) -> None:
    """
    Test ``fetchone``, ``fetchmany``, ``fetchall``.
    """
    requests = mocker.patch("dj.sql.dbapi.cursor.requests")
    requests.post().headers.get.return_value = "application/json"
    requests.post().json.return_value = {
        "database_id": 1,
        "catalog": None,
        "schema_": None,
        "id": "3d33ceae-3484-45b6-807f-7c7cea3f6577",
        "submitted_query": "SELECT 1",
        "executed_query": "SELECT 1",
        "scheduled": "2022-04-08T18:24:06.395989",
        "started": "2022-04-08T18:24:06.396026",
        "finished": "2022-04-08T18:24:06.396882",
        "state": "FINISHED",
        "progress": 1.0,
        "results": [
            {
                "sql": "SELECT COUNT(*) AS A FROM B GROUP BY B.group",
                "columns": [{"name": "A", "type": "INT"}],
                "rows": [[1], [2], [3]],
                "row_count": 3,
            },
        ],
        "next": None,
        "previous": None,
        "errors": [],
    }
    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    cursor.execute("SELECT A FROM metrics GROUP BY B.group")
    assert cursor.fetchone() == (1,)
    assert cursor.fetchone() == (2,)
    assert cursor.fetchone() == (3,)
    assert cursor.fetchone() is None

    cursor.execute("SELECT A FROM metrics GROUP BY B.group")
    assert cursor.fetchmany(2) == [(1,), (2,)]
    assert cursor.fetchmany(2) == [(3,)]

    cursor.execute("SELECT A FROM metrics GROUP BY B.group")
    assert cursor.fetchall() == [(1,), (2,), (3,)]


def test_fetch_before_execute() -> None:
    """
    Test that an exception is raised when fetching results before executing query.
    """
    url = URL("http://localhost:8000/")
    cursor = Cursor(url)

    with pytest.raises(ProgrammingError) as excinfo:
        cursor.fetchall()
    assert str(excinfo.value) == "Called before ``execute``"
