"""
Decorators for the DB API 2.0 implementation.
"""
# pylint: disable=invalid-name, unused-import

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union, cast

from dj.sql.dbapi.exceptions import ProgrammingError

if TYPE_CHECKING:
    from dj.sql.dbapi.connection import Connection
    from dj.sql.dbapi.cursor import Cursor


METHOD = TypeVar("METHOD", bound=Callable[..., Any])


def check_closed(method: METHOD) -> METHOD:
    """
    Decorator that checks if a connection or cursor is closed.
    """

    @wraps(method)
    def wrapper(self: Union["Connection", "Cursor"], *args: Any, **kwargs: Any) -> Any:
        if self.closed:
            raise ProgrammingError(f"{self.__class__.__name__} already closed")
        return method(self, *args, **kwargs)

    return cast(METHOD, wrapper)


def check_result(method: METHOD) -> METHOD:
    """
    Decorator that checks if the cursor has results from ``execute``.
    """

    @wraps(method)
    def wrapper(self: "Cursor", *args: Any, **kwargs: Any) -> Any:
        if self._results is None:  # pylint: disable=protected-access
            raise ProgrammingError("Called before ``execute``")
        return method(self, *args, **kwargs)

    return cast(METHOD, wrapper)
