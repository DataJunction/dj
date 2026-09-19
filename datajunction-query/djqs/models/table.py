"""
Models for use in table API requests and responses
"""

from pydantic import BaseModel


class TableInfo(BaseModel):
    """
    Table information
    """

    name: str
    columns: list[dict[str, str]]
