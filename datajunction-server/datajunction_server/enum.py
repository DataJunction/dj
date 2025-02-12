"""
Backwards-compatible StrEnum for both Python >= and < 3.11
"""

import enum
import sys

if sys.version_info >= (3, 11):
    from enum import (  # pragma: no cover
        IntEnum,
        StrEnum,
    )
else:

    class StrEnum(str, enum.Enum):  # pragma: no cover
        """Backwards compatible StrEnum for Python < 3.11"""  # pragma: no cover

        def __repr__(self):
            return str(self.value)

        def __str__(self):
            return str(self.value)

    class IntEnum(int, enum.Enum):  # pragma: no cover
        """Backwards compatible IntEnum for Python < 3.11"""  # pragma: no cover
