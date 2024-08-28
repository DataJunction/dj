"""
User related scalars
"""
from enum import Enum
from typing import Optional

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
class User:  # pylint: disable=too-few-public-methods
    """
    A DataJunction User
    """

    id: BigInt
    username: str
    email: Optional[str]
    name: Optional[str]
    oauth_provider: OAuthProvider
    is_admin: bool
