"""
An implementation of a DB API 2.0 cursor.
"""
# pylint: disable=invalid-name, unused-import, no-self-use

import itertools
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from sqloxide import parse_sql
from yarl import URL

from datajunction.constants import DJ_DATABASE_ID
from datajunction.errors import DJException
from datajunction.sql.dbapi import exceptions
from datajunction.sql.dbapi.decorators import check_closed, check_result
from datajunction.sql.dbapi.exceptions import (  # pylint: disable=redefined-builtin
    InternalError,
    NotSupportedError,
    ProgrammingError,
    Warning,
)
from datajunction.sql.dbapi.typing import Description
from datajunction.sql.dbapi.utils import escape_parameter
from datajunction.typing import ColumnType


class Cursor:

    """
    Connection cursor.
    """

    def __init__(self, base_url: URL, database_id: int = DJ_DATABASE_ID):
        self.base_url = base_url
        self.database_id = database_id

        self.arraysize = 1
        self.closed = False
        self.description: Description = None

        self._results: Optional[Iterator[Tuple[Any, ...]]] = None
        self._rowcount = -1

    @property  # type: ignore
    @check_closed
    def rowcount(self) -> int:
        """
        Return the number of rows after a query.
        """
        try:
            results = list(self._results)  # type: ignore
        except TypeError:
            return -1

        n = len(results)
        self._results = iter(results)
        return max(0, self._rowcount) + n

    @check_closed
    def close(self) -> None:
        """
        Close the cursor.
        """
        self.closed = True

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> "Cursor":
        """
        Execute a query using the cursor.
        """
        self.description = None
        self._rowcount = -1

        if parameters:
            escaped_parameters = {
                key: escape_parameter(value) for key, value in parameters.items()
            }
            operation %= escaped_parameters

        tree = parse_sql(operation, dialect="ansi")
        if len(tree) > 1:
            raise Warning("You can only execute one statement at a time")

        response = requests.post(
            self.base_url / "queries/",
            json={"database_id": self.database_id, "submitted_query": operation},
            headers={"Content-Type": "application/json"},
        )
        if not response.ok:
            if (
                response.headers.get("X-DJ-Error", "").lower() == "true"
                and response.headers.get("X-DBAPI-Exception")
                and response.headers.get("content-type") == "application/json"
            ):
                exc_name = response.headers["X-DBAPI-Exception"]
                exc = getattr(exceptions, exc_name)
                payload = response.json()
                raise exc(payload["message"])

            raise InternalError(
                "It is pitch black. You are likely to be eaten by a grue.",
            )

        payload = response.json()
        results = payload["results"][0]
        self._results = (tuple(row) for row in results["rows"])
        self.description = [
            (
                column["name"],
                ColumnType(column["type"]),
                None,
                None,
                None,
                None,
                True,
            )
            for column in results["columns"]
        ]

        return self

    @check_closed
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Optional[List[Dict[str, Any]]] = None,
    ) -> "Cursor":
        """
        Execute multiple statements.

        Currently not supported.
        """
        raise NotSupportedError(
            "``executemany`` is not supported, use ``execute`` instead",
        )

    @check_result
    @check_closed
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """
        Fetch the next row of a query result set, returning a single sequence,
        or ``None`` when no more data is available.
        """
        try:
            row = self.next()
        except StopIteration:
            return None

        self._rowcount = max(0, self._rowcount) + 1

        return row

    @check_result
    @check_closed
    def fetchmany(self, size=None) -> List[Tuple[Any, ...]]:
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        results = list(itertools.islice(self, size))

        return results

    @check_result
    @check_closed
    def fetchall(self) -> List[Tuple[Any, ...]]:
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        results = list(self)

        return results

    @check_closed
    def setinputsizes(self, sizes: int) -> None:
        """
        Used before ``execute`` to predefine memory areas for parameters.

        Currently not supported.
        """

    @check_closed
    def setoutputsizes(self, sizes: int) -> None:
        """
        Set a column buffer size for fetches of large columns.

        Currently not supported.
        """

    @check_result
    @check_closed
    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        for row in self._results:  # type: ignore
            self._rowcount = max(0, self._rowcount) + 1
            yield row

    @check_result
    @check_closed
    def __next__(self) -> Tuple[Any, ...]:
        return next(self._results)  # type: ignore

    next = __next__
