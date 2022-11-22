"""
An implementation of a DB API 2.0 connection.
"""
# pylint: disable=invalid-name, unused-import

from typing import Any, Dict, List, Optional, Union

from yarl import URL

from dj.constants import DJ_DATABASE_ID
from dj.sql.dbapi.cursor import Cursor
from dj.sql.dbapi.decorators import check_closed
from dj.sql.dbapi.exceptions import NotSupportedError


class Connection:

    """
    Connection.
    """

    def __init__(self, base_url: URL, database_id: int = DJ_DATABASE_ID):
        self.base_url = base_url
        self.database_id = database_id

        self.closed = False
        self.cursors: List[Cursor] = []

    @check_closed
    def close(self) -> None:
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            if not cursor.closed:
                cursor.close()

    @check_closed
    def commit(self) -> None:
        """Commit any pending transaction to the database."""

    @check_closed
    def rollback(self) -> None:
        """Rollback any transactions."""

    @check_closed
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self.base_url, self.database_id)
        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Cursor:
        """
        Execute a query on a cursor.
        """
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def connect(base_url: Union[str, URL], database_id: int = DJ_DATABASE_ID) -> Connection:
    """
    Create a connection to the database.
    """
    if not isinstance(base_url, URL):
        base_url = URL(base_url)

    return Connection(base_url, database_id)
