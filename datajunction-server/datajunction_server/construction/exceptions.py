"""
Exceptions used in construction
"""

from typing import List, Optional

from datajunction_server.errors import DJError, DJQueryBuildException


class CompoundBuildException:
    """
    Exception singleton to optionally build up exceptions or raise
    """

    errors: List[DJError]
    _instance: Optional["CompoundBuildException"] = None
    _raise: bool = True

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CompoundBuildException, cls).__new__(
                cls,
                *args,
                **kwargs,
            )
            cls.errors = []
        return cls._instance

    def reset(self):
        """
        Resets the singleton
        """
        self._raise = True
        self.errors = []

    def set_raise(self, raise_: bool):
        """
        Set whether to raise caught exceptions or accumulate them
        """
        self._raise = raise_

    def append(self, error: DJError, message: Optional[str] = None):
        """
        Accumulate DJ exceptions
        """
        if self._raise:
            raise DJQueryBuildException(
                message=message or error.message,
                errors=[error],
            )
        self.errors.append(error)

    def __str__(self) -> str:
        plural = "s" if len(self.errors) > 1 else ""
        error = f"Found {len(self.errors)} issue{plural}:\n"
        return error + "\n\n".join(
            "\t" + str(type(exc).__name__) + ": " + str(exc) + "\n" + "=" * 50
            for exc in self.errors
        )
