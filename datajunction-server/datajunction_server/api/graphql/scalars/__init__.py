"""
GraphQL scalars
"""
from typing import Union

import strawberry

BigInt = strawberry.scalar(
    Union[int, str],  # type: ignore
    serialize=int,
    parse_value=str,
    description="BigInt field",
)
