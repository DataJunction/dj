"""
GQL Database Models and related APIs.
"""
from typing_extensions import Annotated
from typing import TYPE_CHECKING, List

import strawberry
from sqlmodel import select
from strawberry.scalars import JSON
from strawberry.types import Info

from dj.models.database import Database as _Database

if TYPE_CHECKING:
    from dj.api.graphql.query import Query_
    from dj.api.graphql.table import Table


@strawberry.experimental.pydantic.type(
    model=_Database,
    fields=[
        "id",
        "name",
        "description",
        "URI",
        "read_only",
        "async_",
        "cost",
        "created_at",
        "updated_at",
    ],
)
class Database:
    """
    Class for a database.
    """

    extra_params: JSON
    tables: List[Annotated["Table", strawberry.lazy("dj.api.graphql.table")]]
    queries: List[
        Annotated["Query_", strawberry.lazy("dj.api.graphql.query")]
    ]


def get_databases(info: Info) -> List[Database]:
    """
    List the available databases.
    """
    dbs = info.context["session"].exec(select(_Database)).all()
    return [Database.from_pydantic(db) for db in dbs]
