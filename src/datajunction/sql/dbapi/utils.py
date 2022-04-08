"""
Helper functions et al.
"""

from typing import Any


def escape_parameter(value: Any) -> str:
    """
    Escape a query parameter.
    """
    if value == "*":
        return value
    if isinstance(value, str):
        value = value.replace("'", "''")
        return f"'{value}'"
    if isinstance(value, bytes):
        value = value.decode("utf-8")
        return f"'{value}'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{value}'"
