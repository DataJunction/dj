"""
GitRepositoryInfo GraphQL scalar
"""

import strawberry
from datajunction_server.models.node import (
    GitRepositoryInfo as PydanticGitRepositoryInfo,
)


@strawberry.experimental.pydantic.type(
    model=PydanticGitRepositoryInfo,
    all_fields=True,
)
class GitRepositoryInfo:
    """
    Git repository information for a namespace
    """
