"""
GQL Models for queries.
"""

# pylint: disable=too-few-public-methods, no-member

from typing import Any, List, NewType, Union

import strawberry
from strawberry.types import Info

from dj.api.queries import save_query_and_run
from dj.models.query import BaseQuery as BaseQuery_
from dj.models.query import ColumnMetadata as ColumnMetadata_
from dj.models.query import Query as Query_
from dj.models.query import QueryCreate as QueryCreate_
from dj.models.query import QueryExtType as QueryExtType_
from dj.models.query import QueryResults as QueryResults_
from dj.models.query import QueryWithResults as QueryWithResults_
from dj.models.query import StatementResults as StatementResults_
from dj.typing import QueryState as QueryState_

QueryState = strawberry.enum(QueryState_)


@strawberry.experimental.pydantic.type(model=BaseQuery_, all_fields=True)
class BaseQuery:
    """
    Base class for query models.
    """


@strawberry.experimental.pydantic.type(model=Query_, all_fields=True)
class Query:  # pylint: disable=invalid-name
    """
    A query.
    """


@strawberry.experimental.pydantic.input(model=QueryCreate_, all_fields=True)
class QueryCreate:
    """
    Model for submitted queries.
    """


@strawberry.experimental.pydantic.type(model=ColumnMetadata_, all_fields=True)
class ColumnMetadata:
    """
    A simple model for column metadata.
    """


def row_serializer(row: List[Any]) -> List[Union[int, float, str]]:
    """
    Serialize values in a row into numbers or strings.
    """

    def transform(value: Any) -> Union[int, float, str]:
        """
        Convert values to numbers or strings.
        """
        return value if isinstance(value, (int, float)) else repr(value)

    return [transform(value) for value in row]


Row = strawberry.scalar(  # pragma: no cover
    NewType("Row", List[List[Union[int, float, str]]]),
    serialize=row_serializer,
    parse_value=lambda v: v,
)


@strawberry.experimental.pydantic.type(
    model=StatementResults_,
    fields=["sql", "columns", "row_count"],
)
class StatementResults:
    """
    Results for a given statement.

    This contains the SQL, column names and types, and rows
    """

    rows: List[Row]  # type: ignore


@strawberry.experimental.pydantic.type(model=QueryResults_, all_fields=True)
class QueryResults:
    """
    Results for a given query.
    """


@strawberry.experimental.pydantic.type(model=QueryWithResults_, all_fields=True)
class QueryWithResults:
    """
    Model for query with results.
    """


QueryExtType = strawberry.enum(QueryExtType_)


async def submit_query(create_query: QueryCreate, info: Info) -> QueryWithResults:
    """
    Submit a query.
    """
    query_with_results = save_query_and_run(
        create_query.to_pydantic(),  # type: ignore
        info.context["session"],
        info.context["settings"],
        info.context["response"],
        info.context["background_tasks"],
    )
    return QueryWithResults.from_pydantic(query_with_results)  # type: ignore
