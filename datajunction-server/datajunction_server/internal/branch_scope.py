"""
Names a branch inherits from its parent.
"""

from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TypeVar

from sqlalchemy import ColumnElement, delete, exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision, _name_in
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR

T = TypeVar("T")


async def parent_node_names(
    session: AsyncSession,
    names: list[str],
) -> dict[str, str]:
    """
    Maps node names in a branch to the same nodes in the default branch.

    Names outside a branch, or inside the default branch, are left out.
    """
    from datajunction_server.internal.namespaces import get_git_info_for_namespaces

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


async def fetch_with_inherited(
    session: AsyncSession,
    names: list[str],
    fetch: Callable[[list[str]], Awaitable[dict[str, T]]],
) -> dict[str, T]:
    """
    Runs ``fetch`` by name, then again for default-branch names of misses.

    Results are keyed by the name that was asked for.
    """
    results = await fetch(names)
    missing = [name for name in names if name not in results]
    if not missing:
        return results
    inherited = await inherited_names(session, missing)
    if not inherited:
        return results
    parent_results = await fetch(list(inherited.values()))
    for name, parent in inherited.items():
        if parent in parent_results:
            results[name] = parent_results[parent]
    return results


def placeholder_clause() -> ColumnElement[bool]:
    """
    Matches placeholder rows: deactivated nodes that have no revisions.
    """
    return Node.deactivated_at.is_not(None) & ~exists().where(
        NodeRevision.node_id == Node.id,
    )


async def leave_placeholders(
    session: AsyncSession,
    deleted: dict[str, NodeType],
    created_by_id: int,
) -> list[str]:
    """
    Adds a placeholder row for each deleted branch node the default branch holds.

    Returns the names that got a placeholder.
    """
    parents = await parent_node_names(session, list(deleted))
    if not parents:
        return []
    held = set(
        (
            await session.execute(
                select(Node.name).where(
                    _name_in(list(parents.values())),
                    Node.deactivated_at.is_(None),
                ),
            )
        )
        .scalars()
        .all(),
    )
    names = sorted(name for name, parent in parents.items() if parent in held)
    deactivated_at = datetime.now(UTC)
    session.add_all(
        Node(
            name=name,
            type=deleted[name],
            namespace=name.rpartition(SEPARATOR)[0],
            created_by_id=created_by_id,
            deactivated_at=deactivated_at,
        )
        for name in names
    )
    await session.flush()
    return names


async def drop_placeholders(session: AsyncSession, names: list[str]) -> list[str]:
    """
    Removes the placeholder rows among ``names``.

    Returns the names that were removed.
    """
    if not names:
        return []
    result = await session.execute(
        delete(Node)
        .where(_name_in(names), placeholder_clause())
        .returning(Node.name)
        .execution_options(synchronize_session="fetch"),
    )
    return sorted(result.scalars().all())
