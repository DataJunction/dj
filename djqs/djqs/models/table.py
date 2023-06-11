"""
Models for use in table API requests and responses
"""
from typing import Dict, List

from pydantic import BaseModel


class TableInfo(BaseModel):
    """
    Table information
    """

    name: str
    columns: List[Dict[str, str]]
