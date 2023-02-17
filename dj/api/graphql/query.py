"""
GQL Models for queries.
"""

# pylint: disable=too-few-public-methods, no-member

from typing import Any, List, NewType, Union

import strawberry
from strawberry.types import Info

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



