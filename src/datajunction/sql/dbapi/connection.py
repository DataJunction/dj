"""
An implementation of a DB API 2.0 connection.
"""
# pylint: disable=invalid-name, unused-import, no-self-use

from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from yarl import URL

from datajunction.constants import DJ_DATABASE_UUID
from datajunction.sql.dbapi.cursor import Cursor
from datajunction.sql.dbapi.decorators import check_closed
from datajunction.sql.dbapi.exceptions import NotSupportedError


class Connection:

    """
    Connection.
    """

    def __init__(self, base_url: URL, database_uuid: UUID = DJ_DATABASE_UUID):
        self.base_url = base_url
        self.database_uuid = database_uuid

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
        cursor = Cursor(self.base_url, self.database_uuid)
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


def connect(
    base_url: Union[str, URL],
    database_uuid: UUID = DJ_DATABASE_UUID,
) -> Connection:
    """
    Create a connection to the database.
    """
    if not isinstance(base_url, URL):
        base_url = URL(base_url)

    return Connection(base_url, database_uuid)
