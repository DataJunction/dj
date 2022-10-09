"""
Type annotations for the DB API 2.0 implementation.
"""

from typing import List, Optional, Tuple

from dj.typing import ColumnType

# Cursor description
Description = Optional[
    List[
        Tuple[
            str,
            ColumnType,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[bool],
        ]
    ]
]
