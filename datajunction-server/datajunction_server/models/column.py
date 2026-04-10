"""
Models for columns.
"""

from functools import lru_cache
from typing import Optional, TypedDict

from pydantic.main import BaseModel
from sqlalchemy import TypeDecorator
from sqlalchemy.types import Text

from datajunction_server.enum import StrEnum
from datajunction_server.sql.parsing.types import ColumnType


@lru_cache(maxsize=1024)
def _parse_column_type(value: str) -> ColumnType:
    """Parse a column type string via ANTLR4, with process-level caching.

    Column types repeat heavily across nodes (BIGINT, VARCHAR, DOUBLE, etc.).
    Caching eliminates redundant ANTLR4 invocations on DB load.
    """
    from datajunction_server.sql.parsing.backends.antlr4 import parse_rule

    return parse_rule(value, "dataType")  # type: ignore[return-value]


_COMMON_COLUMN_TYPES = [
    "boolean",
    "int",
    "tinyint",
    "smallint",
    "bigint",
    "float",
    "double",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "string",
    "varchar",
    "binary",
    "uuid",
]


def warm_column_type_cache() -> None:
    """Pre-populate the column type cache with common primitive types.

    Called at server startup so routine DB loads never hit ANTLR4.
    Only exotic types (map<...>, array<...>, struct<...>) will parse on first use.
    """
    for type_str in _COMMON_COLUMN_TYPES:  # pragma: no cover
        _parse_column_type(type_str)


class ColumnYAML(TypedDict, total=False):
    """
    Schema of a column in the YAML file.
    """

    type: str
    dimension: str
    description: str


class ColumnTypeDecorator(TypeDecorator):
    """
    Converts a column type from the database to a `ColumnType` class
    """

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: ColumnType, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value
        try:
            return _parse_column_type(value)
        except Exception:  # pragma: no cover
            return value  # pragma: no cover


class ColumnAttributeInput(BaseModel):
    """
    A column attribute input
    """

    attribute_type_namespace: Optional[str] = "system"
    attribute_type_name: str
    column_name: str


class SemanticType(StrEnum):
    """
    Semantic type of a column
    """

    MEASURE = "measure"
    METRIC = "metric"
    DIMENSION = "dimension"
    TIMESTAMP = "timestamp"
