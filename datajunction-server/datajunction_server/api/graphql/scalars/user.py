"""
User related scalars
"""

from enum import Enum

import strawberry

from datajunction_server.api.graphql.scalars import BigInt


@strawberry.enum
class OAuthProvider(Enum):
    """
    An oauth implementation provider
    """

    BASIC = "basic"
    GITHUB = "github"
    GOOGLE = "google"


@strawberry.type
class User:
    """
    A DataJunction User
    """

    id: BigInt
    username: str
    email: str | None
    name: str | None
    oauth_provider: OAuthProvider
    is_admin: bool
