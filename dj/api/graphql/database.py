"""
GQL Database Models and related APIs.
"""

# pylint: disable=too-few-public-methods, no-member

from typing import TYPE_CHECKING, List

import strawberry
from sqlmodel import select
from strawberry.scalars import JSON
from strawberry.types import Info
from typing_extensions import Annotated

from dj.models.database import Database as Database_

if TYPE_CHECKING:
    from dj.api.graphql.query import Query
    from dj.api.graphql.table import Table


@strawberry.experimental.pydantic.type(
    model=Database_,
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
class Database:  # pylint: disable=too-few-public-methods
    """
    Class for a database.
    """

    extra_params: JSON
    tables: List[Annotated["Table", strawberry.lazy("dj.api.graphql.table")]]
    queries: List[Annotated["Query", strawberry.lazy("dj.api.graphql.query")]]


def get_databases(info: Info) -> List[Database]:
    """
    List the available databases.
    """
    dbs = info.context["session"].exec(select(Database_)).all()
    return [Database.from_pydantic(db) for db in dbs]  # type: ignore
