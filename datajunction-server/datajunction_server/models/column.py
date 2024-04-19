"""
Models for columns.
"""
from typing import Optional, TypedDict

from pydantic.main import BaseModel
from sqlalchemy import TypeDecorator
from sqlalchemy.types import Text

from datajunction_server.enum import StrEnum
from datajunction_server.sql.parsing.types import ColumnType


class ColumnYAML(TypedDict, total=False):
    """
    Schema of a column in the YAML file.
    """

    type: str
    dimension: str


class ColumnTypeDecorator(
    TypeDecorator,
):  # pylint: disable=abstract-method, too-many-ancestors
    """
    Converts a column type from the database to a `ColumnType` class
    """

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: ColumnType, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse_rule,
        )

        if not value:
            return value
        return parse_rule(value, "dataType")


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
