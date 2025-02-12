"""
Models for tables.
"""

from typing import List, Optional, TypedDict

from pydantic import Field
from pydantic.main import BaseModel


class TableYAML(TypedDict, total=False):
    """
    Schema of a table in the YAML file.
    """

    catalog: Optional[str]
    schema: Optional[str]
    table: str
    cost: float


class TableBase(BaseModel):
    """
    A base table.
    """

    schema_: Optional[str] = Field(default=None, alias="schema")
    table: str
    cost: float = 1.0


class CreateColumn(BaseModel):
    """
    A column creation request
    """

    name: str
    type: str


class CreateTable(TableBase):
    """
    Create table input
    """

    database_name: str
    catalog_name: str
    columns: List[CreateColumn]
