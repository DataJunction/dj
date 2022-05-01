"""
Test compliance with PEP 249 (https://www.python.org/dev/peps/pep-0249/).
"""

from inspect import isfunction, ismethod

import pytest
from requests_mock.mocker import Mocker

from datajunction.sql import dbapi


def test_module_interface() -> None:
    """
    Test constructors, globals, and exceptions.
    """
    # constructors
    assert isfunction(dbapi.connect)

    # globals
    assert dbapi.apilevel == "2.0"
    assert dbapi.threadsafety == 3
    assert dbapi.paramstyle == "pyformat"

    # exceptions
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.Error, Exception)
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    assert issubclass(dbapi.DataError, dbapi.DatabaseError)
    assert issubclass(dbapi.OperationalError, dbapi.DatabaseError)
    assert issubclass(dbapi.IntegrityError, dbapi.DatabaseError)
    assert issubclass(dbapi.InternalError, dbapi.DatabaseError)
    assert issubclass(dbapi.ProgrammingError, dbapi.DatabaseError)
    assert issubclass(dbapi.NotSupportedError, dbapi.DatabaseError)


def test_connection() -> None:
    """
    Test that the connection object implements required methods.
    """
    connection = dbapi.connect("http://localhost:8000/")

    assert ismethod(connection.close)
    assert ismethod(connection.commit)
    assert ismethod(connection.rollback)
    assert ismethod(connection.cursor)


def test_cursor(requests_mock: Mocker) -> None:
    """
    Test that the cursor implements required methods.
    """
    requests_mock.post(
        "http://localhost:8000/queries/",
        json={
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
                    "sql": "SELECT 1",
                    "columns": [{"name": "1", "type": "STR"}],
                    "rows": [[1]],
                    "row_count": 1,
                },
            ],
            "next": None,
            "previous": None,
            "errors": [],
        },
        headers={"Content-Type": "application/json"},
    )

    connection = dbapi.connect("http://localhost:8000/")
    cursor = connection.cursor()

    # attributes
    assert cursor.description is None
    assert cursor.rowcount == -1

    cursor.execute("SELECT 1")
    assert cursor.description
    assert len(cursor.description) == 1
    assert all(len(sequence) == 7 for sequence in cursor.description)
    assert cursor.rowcount == 1

    # methods
    assert ismethod(cursor.close)
    cursor.close()
    with pytest.raises(dbapi.Error) as excinfo:
        cursor.execute("SELECT 1")
    assert str(excinfo.value) == "Cursor already closed"

    assert ismethod(cursor.execute)
    assert ismethod(cursor.executemany)
    assert ismethod(cursor.fetchone)
    assert ismethod(cursor.fetchmany)
    assert ismethod(cursor.fetchall)
    assert cursor.arraysize == 1
    cursor.arraysize = 2
    assert cursor.arraysize == 2
    assert ismethod(cursor.setinputsizes)
    assert ismethod(cursor.setoutputsizes)


def test_type_objects_and_constructors() -> None:
    """
    Test type objects and constructors.
    """
    assert isfunction(dbapi.Date)
    assert isfunction(dbapi.Time)
    assert isfunction(dbapi.Timestamp)
    assert isfunction(dbapi.DateFromTicks)
    assert isfunction(dbapi.TimeFromTicks)
    assert isfunction(dbapi.TimestampFromTicks)
    assert isfunction(dbapi.Binary)
    assert dbapi.STRING
    assert dbapi.BINARY
    assert dbapi.NUMBER
    assert dbapi.DATETIME
