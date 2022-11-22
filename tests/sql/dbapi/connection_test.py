"""
Tests for ``djqs.sql.dbapi.connection``.
"""
# pylint: disable=redefined-builtin, invalid-name

from pytest_mock import MockerFixture
from yarl import URL

from djqs.sql.dbapi.connection import connect


def test_connection() -> None:
    """
    Basic tests for the connection.
    """
    connection = connect("http://localhost:8001/")
    assert not connection.closed
    connection.close()
    assert connection.closed

    connection = connect("http://localhost:8001/")
    assert not connection.cursors
    cursor_1 = connection.cursor()
    cursor_2 = connection.cursor()
    assert connection.cursors == [cursor_1, cursor_2]
    cursor_2.close()
    connection.close()
    assert cursor_1.closed
    assert cursor_2.closed

    connection = connect(URL("http://localhost:8001/"))

    # these are no-ops
    assert connection.commit() is None
    assert connection.rollback() is None

    with connection:
        assert not connection.closed
    assert connection.closed


def test_connection_execute(mocker: MockerFixture) -> None:
    """
    Test the ``execute`` method.
    """
    Cursor = mocker.patch("djqs.sql.dbapi.connection.Cursor")

    connection = connect("http://localhost:8001/")
    connection.execute(
        "SELECT * FROM some_table WHERE name = %(name)s",
        {"name": "Alice"},
    )

    Cursor().execute.assert_called_with(
        "SELECT * FROM some_table WHERE name = %(name)s",
        {"name": "Alice"},
    )
