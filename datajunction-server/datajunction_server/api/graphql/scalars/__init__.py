"""
GraphQL scalars
"""
from typing import Optional, Union

import strawberry

BigInt = strawberry.scalar(
    Union[int, str],  # type: ignore
    serialize=int,
    parse_value=str,
    description="BigInt field",
)


@strawberry.type
class PageMeta:  # pylint: disable=too-few-public-methods
    """
    Pagination-related metadata
    """

    prev_cursor: Optional[str] = strawberry.field(description="The previous cursor")
    next_cursor: Optional[str] = strawberry.field(
        description="The next cursor to continue with",
    )
    count: int = strawberry.field(description="The number of items in this page")
