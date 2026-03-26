"""
Namespace GraphQL queries.
"""

from typing import Optional, Union

from sqlalchemy import func, select
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.namespace import (
    GitBranchConfig,
    GitRootConfig,
    Namespace,
)
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node


async def list_namespaces(
    *,
    info: Info = None,
) -> list[Namespace]:
    """
    List all active namespaces with node counts and git configuration.

    For git root namespaces, git is a GitRootConfig.
    For branch namespaces, git is a GitBranchConfig with the root config embedded.
    For non-git namespaces, git is null.
    """
    session = info.context["session"]  # type: ignore
    statement = (
        select(NodeNamespace, func.count(Node.id).label("num_nodes"))
        .join(Node, onclause=NodeNamespace.namespace == Node.namespace, isouter=True)
        .where(NodeNamespace.deactivated_at.is_(None))
        .group_by(NodeNamespace.namespace)
    )
    result = await session.execute(statement)
    rows = result.all()

    # Build a map so branch namespaces can resolve their root config inline
    ns_map = {ns.namespace: ns for ns, _ in rows}

    namespaces = []
    for ns, num_nodes in rows:
        git: Optional[Union[GitRootConfig, GitBranchConfig]] = None
        if ns.github_repo_path:
            git = GitRootConfig(  # type: ignore
                repo=ns.github_repo_path,
                path=ns.git_path,
                default_branch=ns.default_branch,
            )
        elif ns.git_branch and ns.parent_namespace:
            parent = ns_map.get(ns.parent_namespace)
            if parent and parent.github_repo_path:  # pragma: no branch
                git = GitBranchConfig(  # type: ignore
                    branch=ns.git_branch,
                    git_only=ns.git_only,
                    parent_namespace=ns.parent_namespace,
                    root=GitRootConfig(  # type: ignore
                        repo=parent.github_repo_path,
                        path=parent.git_path,
                        default_branch=parent.default_branch,
                    ),
                )
        namespaces.append(
            Namespace(  # type: ignore
                namespace=ns.namespace,
                num_nodes=num_nodes or 0,
                git=git,
            ),
        )
    return namespaces
