"""
Models for tables.
"""

from typing import TYPE_CHECKING, Annotated, List

import strawberry

from datajunction.api.graphql.column import Column
from datajunction.api.graphql.node import Node
from datajunction.models.table import Table as _Table
from datajunction.models.table import TableColumns as _TableColumns


@strawberry.experimental.pydantic.type(model=_TableColumns, all_fields=True)
class TableColumns:
    """
    Join table for table columns.
    """


if TYPE_CHECKING:
    from datajunction.api.graphql.database import Database


@strawberry.experimental.pydantic.type(
    model=_Table, fields=["id", "node_id", "database_id"]
)
class Table:
    """
    A table with data.

    Nodes can data in multiple tables, in different databases.
    """

    node: Node
    database: Annotated[
        "Database", strawberry.lazy("datajunction.api.graphql.database")
    ]
    columns: List[Column]
