"""
Read-only reconciliation helpers for the tags a deployment manages.

Nothing here mutates tags: a deploy is upsert-only for tags, and this module
exists purely to report the ones a spec has stopped declaring.
"""

from collections.abc import Iterable

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.database.tag import Tag, TagNodeRelationship

TAG_PREFIX_SEPARATOR = ":"


def managed_tag_prefixes(declared_tag_names: Iterable[str]) -> set[str]:
    """
    The tag prefixes a deployment appears to manage, inferred from the
    ``prefix:name`` convention in the tag names it declares.

    This is the one place the scope of tag reconciliation is decided. Tag names
    are globally unique with no owning namespace, so ownership is inferred here
    rather than read from a stored claim; when that ownership model is settled
    this function is what changes.
    """
    return {
        name.split(TAG_PREFIX_SEPARATOR, 1)[0]
        for name in declared_tag_names
        if TAG_PREFIX_SEPARATOR in name
    }


async def find_undeclared_managed_tags(
    session: AsyncSession,
    declared_tag_names: Iterable[str],
) -> list[tuple[str, int]]:
    """
    Tags under a prefix the deployment manages that it no longer declares,
    paired with the number of non-deactivated nodes still attached to each.

    Node counts are aggregated in the database — the tag's ``nodes``
    relationship is never loaded.
    """
    declared = set(declared_tag_names)
    prefixes = managed_tag_prefixes(declared)
    if not prefixes:
        return []

    statement = (
        select(Tag.name, func.count(Node.id))
        .select_from(Tag)
        .outerjoin(TagNodeRelationship, TagNodeRelationship.tag_id == Tag.id)
        .outerjoin(
            Node,
            and_(
                Node.id == TagNodeRelationship.node_id,
                Node.deactivated_at.is_(None),
            ),
        )
        .where(
            or_(
                *[
                    Tag.name.like(f"{prefix}{TAG_PREFIX_SEPARATOR}%")
                    for prefix in sorted(prefixes)
                ],
            ),
            Tag.name.notin_(sorted(declared)),
        )
        .group_by(Tag.name)
        .order_by(Tag.name)
    )
    return [
        (name, node_count)
        for name, node_count in (await session.execute(statement)).all()
    ]
