"""
GQL Models for queries.
"""

from typing import List, NewType, Union

import strawberry
from strawberry.types import Info

from datajunction.api.graphql.database import Database
from datajunction.api.queries import save_query_and_run
from datajunction.models.query import BaseQuery as _BaseQuery
from datajunction.models.query import ColumnMetadata as _ColumnMetadata
from datajunction.models.query import Query as _Query
from datajunction.models.query import QueryCreate as _QueryCreate
from datajunction.models.query import QueryExtType as _QueryExtType
from datajunction.models.query import QueryResults as _QueryResults
from datajunction.models.query import QueryWithResults as _QueryWithResults
from datajunction.models.query import StatementResults as _StatementResults
from datajunction.typing import QueryState as _QueryState

QueryState = strawberry.enum(_QueryState)


@strawberry.experimental.pydantic.type(model=_BaseQuery, all_fields=True)
class BaseQuery:
    """
    Base class for query models.
    """


@strawberry.experimental.pydantic.type(model=_Query, all_fields=True)
class Query_:
    """
    A query.
    """


@strawberry.experimental.pydantic.input(model=_QueryCreate, all_fields=True)
class QueryCreate:
    """
    Model for submitted queries.
    """


@strawberry.experimental.pydantic.type(model=_ColumnMetadata, all_fields=True)
class ColumnMetadata:
    """
    A simple model for column metadata.
    """


def row_serializer(l):
    def tfm(v):
        """
        anything not a number becomes a string
        """
        if type(v) in (int, float):
            return v
        return repr(v)  # pragma: no cover

    return [tfm(v) for v in l]


Row = strawberry.scalar(  # pragma: no cover
    NewType("Row", List[List[Union[int, float, str]]]),
    serialize=row_serializer,
    parse_value=lambda v: v,
)


@strawberry.experimental.pydantic.type(
    model=_StatementResults, fields=["sql", "columns", "row_count"]
)
class StatementResults:
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    rows: List[Row]


@strawberry.experimental.pydantic.type(model=_QueryResults, all_fields=True)
class QueryResults:
    """
    Results for a given query.
    """


@strawberry.experimental.pydantic.type(model=_QueryWithResults, all_fields=True)
class QueryWithResults:
    """
    Model for query with results.
    """


QueryExtType = strawberry.enum(_QueryExtType)


async def submit_query(create_query: QueryCreate, info: Info) -> QueryWithResults:
    query_with_results = save_query_and_run(
        create_query.to_pydantic(),
        info.context["session"],
        info.context["settings"],
        info.context["response"],
        info.context["background_tasks"],
    )
    return QueryWithResults.from_pydantic(query_with_results)
