"""
Backwards-compatible StrEnum for both Python >= and < 3.11
"""
import enum
import sys

if sys.version_info >= (3, 11):
    from enum import IntEnum  # noqa  # pylint: disable=unused-import
    from enum import StrEnum  # noqa  # pylint: disable=unused-import
else:

    class StrEnum(str, enum.Enum):
        """Backwards compatible StrEnum for Python < 3.11"""

        def __str__(self):
            return str(self.value)

    class IntEnum(int, enum.Enum):
        """Backwards compatible IntEnum for Python < 3.11"""
