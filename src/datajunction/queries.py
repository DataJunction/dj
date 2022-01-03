"""
Query related functions.
"""
from enum import Enum
from typing import Any, Iterator, List, Optional, Tuple

import sqlparse
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

    For now this uses the information from the cursor description, which only allow us to
    distinguish between 4 types (see ``TypeEnum``). In the future we should use a type
    inferrer to determine the types based on the query.
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


def run_query(query: Query) -> List[Tuple[List[ColumnMetadata], Stream]]:
    """
    Run a query and return its results.

    For each statement we return a tuple with a description of the columns (name and
    type) and a stream of rows (tuples).
    """
    engine = create_engine(query.database.URI)
    connection = engine.connect()

    output: List[Tuple[List[ColumnMetadata], Stream]] = []
    statements = sqlparse.parse(query.executed_query)
    for statement in statements:
        # Druid doesn't like statements that end in a semicolon...
        sql = str(statement).strip().rstrip(";")

        results = connection.execute(text(sql))
        stream = (tuple(row) for row in results)
        columns = get_columns_from_description(
            results.cursor.description,
            engine.dialect,
        )
        output.append((columns, stream))

    return output
