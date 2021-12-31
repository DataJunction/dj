"""
Query related functions.
"""
from enum import Enum
from typing import Any, Iterator, List, Optional, Tuple

from sqlalchemy import text
from sqlmodel import SQLModel, create_engine

from datajunction.models import Query

Stream = Iterator[Tuple[Any, ...]]

# Cursor description
Description = Optional[
    List[
        Tuple[
            str,
            Any,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[bool],
        ]
    ]
]


class TypeEnum(Enum):
    """
    PEP 249 basic types.

    Unfortunately SQLAlchemy doesn't seem to offer an API for determining the types of the
    columns in a (SQL Core) query, and the DB API 2.0 cursor only offers very coarse
    types.
    """

    STRING = "STRING"
    BINARY = "BINARY"
    NUMBER = "NUMBER"
    DATETIME = "DATETIME"
    UNKNOWN = "UNKNOWN"


class ColumnMetadata(SQLModel):
    """
    A simple model for column metadata.
    """

    name: str
    type: TypeEnum


def get_columns_from_description(
    description: Description,
    dialect: Any,
) -> List[ColumnMetadata]:
    """
    Extract column metadata from the cursor description.
    """
    columns = []
    for column in description or []:
        name, native_type = column[:2]
        for dbapi_type in TypeEnum:
            if native_type == getattr(dialect.dbapi, dbapi_type.value, None):
                type_ = dbapi_type
                break
        else:
            type_ = TypeEnum.UNKNOWN

        columns.append(ColumnMetadata(name=name, type=type_))

    return columns


def run_query(query: Query) -> Tuple[List[ColumnMetadata], Stream]:
    """
    Run a query and return its results.
    """
    engine = create_engine(query.database.URI)
    connection = engine.connect()

    sql = text(query.executed_query)
    results = connection.execute(sql)
    stream = (tuple(row) for row in results)

    columns = get_columns_from_description(results.cursor.description, engine.dialect)

    return columns, stream
