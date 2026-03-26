"""Namespace GraphQL scalar."""

from typing import Annotated, Optional, Union

import strawberry


@strawberry.type
class GitRootConfig:
    """
    Git configuration for a namespace that is a git root.
    Owns the repo reference and canonical config like defaultBranch.
    """

    repo: str
    path: Optional[str]
    default_branch: Optional[str]


@strawberry.type
class GitBranchConfig:
    """
    Git configuration for a namespace that is a branch of a git root.
    gitOnly=True means this branch cannot be edited from the UI.
    """

    branch: str
    git_only: bool
    parent_namespace: str
    root: GitRootConfig


NamespaceGit = Annotated[
    Union[GitRootConfig, GitBranchConfig],
    strawberry.union("NamespaceGit"),
]


@strawberry.type
class Namespace:
    """
    A DJ namespace with node count and git configuration.
    git is null for non-git-associated namespaces.
    Use __typename to distinguish GitRootConfig from GitBranchConfig.
    """

    namespace: str
    num_nodes: int
    git: Optional[NamespaceGit]
