"""
Names a branch inherits from its parent.
"""

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, _name_in
from datajunction_server.internal.namespaces import get_git_info_for_namespaces
from datajunction_server.utils import SEPARATOR


async def parent_node_names(
    session: AsyncSession,
    names: list[str],
) -> dict[str, str]:
    """
    Maps node names in a branch to the same nodes in the default branch.

    Names outside a branch, or inside the default branch, are left out.
    """
    by_namespace: dict[str, list[str]] = defaultdict(list)
    for name in names:
        namespace, _, _ = name.rpartition(SEPARATOR)
        if namespace:
            by_namespace[namespace].append(name)

    if not by_namespace:
        return {}
    git_infos = await get_git_info_for_namespaces(session, list(by_namespace))
    parents: dict[str, str] = {}
    for namespace, namespace_names in by_namespace.items():
        git_info = git_infos[namespace]
        if not git_info or git_info["is_default_branch"]:
            continue
        branch = git_info["branch_namespace"]
        root = git_info["parent_namespace"]
        default_branch = git_info["default_branch"]
        if not (branch and root and default_branch):
            continue
        default_namespace = (
            f"{root}{SEPARATOR}{default_branch.replace('-', '_').replace('/', '_')}"
        )
        for name in namespace_names:
            parents[name] = default_namespace + name[len(branch) :]
    return parents


async def inherited_names(
    session: AsyncSession,
    names: list[str],
) -> dict[str, str]:
    """
    Maps missing branch node names to the default branch's names.

    A name the branch holds a row for, even a deactivated one, is left out.
    """
    parents = await parent_node_names(session, names)
    if not parents:
        return {}
    held = set(
        (await session.execute(select(Node.name).where(_name_in(list(parents)))))
        .scalars()
        .all(),
    )
    return {name: parent for name, parent in parents.items() if name not in held}
